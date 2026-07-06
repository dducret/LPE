use axum::body::{to_bytes, Body};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};
use lpe_mail_auth::{AccountAuthStore, AccountPrincipal, StoreFuture};
use lpe_storage::RecoverableItem;
use lpe_storage::{
    AccessibleContact, AccessibleEvent, AccountLogin, ActiveSyncAttachment,
    ActiveSyncAttachmentContent, AttachmentUploadInput, AuthenticatedAccount,
    CalendarEventAttachment, CancelSubmissionResult, ClientNote, ClientReminder, ClientTask,
    CollaborationCollection, CollaborationGrant, CollaborationGrantInput,
    CollaborationResourceKind, CollaborationRights, ConversationAction, CreatePublicFolderInput,
    DelegateFreeBusyMessageObject, JmapEmail, JmapEmailAddress, JmapEmailMailboxState,
    JmapEmailQuery, JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, JournalEntry, MailboxRule, ManagedRetentionFolderCreateInput,
    PublicFolder, PublicFolderItem, PublicFolderPerUserState, PublicFolderPerUserStatePatch,
    PublicFolderPermission, PublicFolderPermissionInput, PublicFolderReplica, PublicFolderRights,
    PublicFolderTree, ReminderQuery, SavedDraftMessage, SearchFolderDefinition,
    SenderDelegationGrantInput, SenderDelegationRight, SieveScriptDocument, Storage,
    StoredAccountAppPassword, SubmitMessageInput, SubmittedMessage, SubmittedRecipientInput,
    UpdatePublicFolderInput, UpsertClientContactInput, UpsertClientEventInput,
    UpsertClientNoteInput, UpsertClientTaskInput, UpsertConversationActionInput,
    UpsertJournalEntryInput, UpsertPublicFolderItemInput, UpsertSearchFolderInput,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::{
    collections::HashMap,
    env,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use uuid::Uuid;

use crate::{
    mapi::{
        notifications::{MapiNotificationEvent, MapiNotificationKind},
        permissions::{rights_from_grant, MapiFolderPermission},
        properties::{MapiNamedProperty, MapiNamedPropertyKind, MAX_NAMED_PROPERTY_ID},
        MapiEndpoint,
    },
    mapi_mailstore,
    mapi_store::MapiStore,
    service::{
        error_response, is_rpc_proxy_in_data_channel_request, mapi_options_handler,
        mark_rpc_proxy_out_endpoint_bind_ack, rpc_proxy_in_channel_response_for_buffer,
        rpc_proxy_in_channel_response_for_endpoint_query,
        rpc_proxy_in_channel_response_for_endpoint_query_with_store, ExchangeService,
    },
    store::{
        EwsAppMarketplacePolicy, EwsDelegate, EwsDiscoverySearchConfig, EwsDiscoverySearchItem,
        EwsDiscoverySearchResult, EwsHoldMailbox, EwsImGroup, EwsImGroupMember, EwsImList,
        EwsImMemberInput, EwsMailAppInstall, EwsMailAppManifest, EwsMailAppTokenEvent,
        EwsMessageTrackingEvent, EwsMessageTrackingReport, EwsMessageTrackingReportDetail,
        EwsNonIndexableReport, EwsRetentionPolicyTag, EwsSearchableMailbox, EwsTransferEntry,
        EwsTransferJob, EwsUnifiedMessagingCall, EwsUserConfiguration, EwsUserConfigurationKey,
        ExchangeAddressBookDirectoryKind, ExchangeAddressBookEntry,
        ExchangeAddressBookEntryDetails, ExchangeAddressBookEntryKind, ExchangeStore,
        MapiCheckpointKind, MapiContentTableQuery, MapiContentTableQueryResult,
        MapiContentTableSortField, MapiCustomPropertyObjectKind, MapiCustomPropertyValue,
        MapiFolderProfilePropertyValue, MapiIdentityLookupRecord, MapiIdentityObjectKind,
        MapiIdentityRecord, MapiIdentityRequest, MapiNamedPropertyMapping, MapiNotificationPoll,
        MapiSyncChangeSet, MapiSyncCheckpoint, UpsertEwsDelegateInput,
        UpsertEwsUserConfigurationInput,
    },
};

static MAPI_TEST_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
const STORAGE_SCHEMA_SQL: &str = include_str!("../../../lpe-storage/sql/schema.sql");

struct PostgresMapiFixture {
    storage: Storage,
    admin_pool: PgPool,
    schema_name: String,
    account_id: Uuid,
}

impl PostgresMapiFixture {
    async fn cleanup(self) -> anyhow::Result<()> {
        self.storage.pool().close().await;
        sqlx::query(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            self.schema_name
        ))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

async fn postgres_mapi_calendar_fixture() -> anyhow::Result<Option<PostgresMapiFixture>> {
    let Some(database_url) = env::var("TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("skipping PostgreSQL-backed MAPI calendar test; TEST_DATABASE_URL is not set");
        return Ok(None);
    };

    let schema_name = format!("lpe_mapi_calendar_{}", Uuid::new_v4().simple());
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(&database_url)?)
        .await?;
    sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public")
        .execute(&admin_pool)
        .await?;
    sqlx::query(&format!("CREATE SCHEMA {schema_name}"))
        .execute(&admin_pool)
        .await?;

    let search_path = format!("{schema_name},public");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            PgConnectOptions::from_str(&database_url)?.options([("search_path", &search_path)]),
        )
        .await?;
    sqlx::raw_sql(STORAGE_SCHEMA_SQL).execute(&pool).await?;

    let tenant_id = Uuid::parse_str("10000000-0000-0000-0000-000000000001").unwrap();
    let domain_id = Uuid::parse_str("10000000-0000-0000-0000-000000000002").unwrap();
    let account_id = Uuid::parse_str("10000000-0000-0000-0000-000000000003").unwrap();
    sqlx::query(
        r#"
        INSERT INTO tenants (id, slug, display_name)
        VALUES ($1, 'mapi-calendar', 'MAPI Calendar')
        "#,
    )
    .bind(tenant_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO domains (id, tenant_id, name)
        VALUES ($1, $2, 'example.test')
        "#,
    )
    .bind(domain_id)
    .bind(tenant_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, 'alice@example.test', 'Alice Calendar')
        "#,
    )
    .bind(account_id)
    .bind(tenant_id)
    .bind(domain_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO account_credentials (tenant_id, account_email, password_hash)
        VALUES ($1, 'alice@example.test', 'test-hash')
        "#,
    )
    .bind(tenant_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO account_sessions (id, tenant_id, token, account_email, expires_at)
        VALUES ($1, $2, 'token', 'alice@example.test', NOW() + INTERVAL '1 hour')
        "#,
    )
    .bind(Uuid::parse_str("10000000-0000-0000-0000-000000000004").unwrap())
    .bind(tenant_id)
    .execute(&pool)
    .await?;

    Ok(Some(PostgresMapiFixture {
        storage: Storage::new(pool),
        admin_pool,
        schema_name,
        account_id,
    }))
}

#[tokio::test]
async fn mapi_identity_mapping_survives_restart_style_store_reload() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..FakeStore::default()
    };
    let mailbox = FakeStore::mailbox(
        "10101010-2020-3030-4040-505050505050",
        "custom",
        "Durable IDs",
    );
    let email = FakeStore::email(
        "60606060-7070-8080-9090-a0a0a0a0a0a0",
        &mailbox.id.to_string(),
        "custom",
        "Stable identity",
    );
    store.mailboxes.lock().unwrap().push(mailbox);
    store.emails.lock().unwrap().push(email);

    let first = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();
    let second = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();

    assert_eq!(first.folders()[0].id, second.folders()[0].id);
    assert_eq!(first.messages()[0].id, second.messages()[0].id);
    assert_eq!(
        crate::mapi::identity::object_id_from_long_term_id(
            &crate::mapi::identity::long_term_id_from_object_id(first.messages()[0].id).unwrap()
        ),
        Some(second.messages()[0].id)
    );
}

#[tokio::test]
async fn mapi_inbox_associated_config_bootstrap_inserts_no_defaults() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..FakeStore::default()
    };

    let first = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();
    let second = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();
    let persisted = store
        .fetch_mapi_associated_configs(account.account_id)
        .await
        .unwrap();

    for class in [
        "IPM.Configuration.UMOLK.UserOptions",
        "IPM.Microsoft.FolderDesign.NamedView",
        "IPM.Configuration.EAS",
        "IPM.Configuration.ELC",
        "IPM.RuleOrganizer",
        "IPM.Sharing.Configuration",
        "IPM.Sharing.Index",
        "IPM.Aggregation",
    ] {
        assert_eq!(
            persisted
                .iter()
                .filter(|config| config.message_class == class)
                .count(),
            0
        );
        assert_eq!(
            first
                .associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID)
                .iter()
                .filter(|message| message.message_class == class)
                .count(),
            0
        );
        assert_eq!(
            second
                .associated_config_sync_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID)
                .iter()
                .filter(|message| message.message_class == class)
                .count(),
            0
        );
    }
}

#[tokio::test]
async fn mapi_inbox_associated_config_bootstrap_preserves_existing_persisted_row() {
    let account = FakeStore::account();
    let existing_id = Uuid::from_u128(0x6d617069_7275_6c65_8000_000000000099);
    let store = FakeStore {
        session: Some(account.clone()),
        associated_configs: Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
            id: existing_id,
            account_id: account.account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: "IPM.RuleOrganizer".to_string(),
            subject: "Client Rule Organizer".to_string(),
            properties_json: serde_json::json!({"0x68020102":{"type":"binary","value":"0102"}}),
        }])),
        ..FakeStore::default()
    };

    let snapshot = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();
    let persisted = store
        .fetch_mapi_associated_configs(account.account_id)
        .await
        .unwrap();
    let rule_rows = persisted
        .iter()
        .filter(|config| config.message_class == "IPM.RuleOrganizer")
        .collect::<Vec<_>>();

    assert_eq!(rule_rows.len(), 1);
    assert_eq!(rule_rows[0].id, existing_id);
    assert_eq!(rule_rows[0].subject, "Client Rule Organizer");
    assert_eq!(
        snapshot
            .associated_config_messages_for_folder(crate::mapi::identity::INBOX_FOLDER_ID)
            .iter()
            .find(|message| message.message_class == "IPM.RuleOrganizer")
            .map(|message| message.subject.as_str()),
        Some("Client Rule Organizer")
    );
}

#[tokio::test]
async fn mapi_associated_config_storage_is_account_scoped() {
    let Some(fixture) = postgres_mapi_calendar_fixture().await.unwrap() else {
        return;
    };
    let other_account_id = Uuid::parse_str("10000000-0000-0000-0000-000000000005").unwrap();
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        SELECT $1, tenant_id, primary_domain_id, 'bob@example.test', 'Bob Config'
        FROM accounts
        WHERE id = $2
        "#,
    )
    .bind(other_account_id)
    .bind(fixture.account_id)
    .execute(fixture.storage.pool())
    .await
    .unwrap();

    let config_id = Uuid::parse_str("10000000-0000-0000-0000-000000000006").unwrap();
    fixture
        .storage
        .upsert_mapi_associated_config(crate::store::UpsertMapiAssociatedConfigInput {
            id: Some(config_id),
            account_id: fixture.account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.AccountPrefs".to_string(),
            subject: "IPM.Configuration.AccountPrefs".to_string(),
            properties_json: serde_json::json!({
                "0x7c060003": {"type": "u32", "value": 4}
            }),
        })
        .await
        .unwrap();

    let overwrite = fixture
        .storage
        .upsert_mapi_associated_config(crate::store::UpsertMapiAssociatedConfigInput {
            id: Some(config_id),
            account_id: other_account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.AccountPrefs".to_string(),
            subject: "Cross-account overwrite".to_string(),
            properties_json: serde_json::json!({
                "0x7c060003": {"type": "u32", "value": 8}
            }),
        })
        .await;
    assert!(overwrite.is_err());

    let owner_configs = fixture
        .storage
        .fetch_mapi_associated_configs(fixture.account_id)
        .await
        .unwrap();
    assert_eq!(owner_configs.len(), 1);
    assert_eq!(owner_configs[0].id, config_id);
    assert_eq!(owner_configs[0].subject, "IPM.Configuration.AccountPrefs");
    assert_eq!(
        fixture
            .storage
            .fetch_mapi_associated_configs(other_account_id)
            .await
            .unwrap(),
        Vec::new()
    );

    fixture.cleanup().await.unwrap();
}

#[tokio::test]
async fn mapi_associated_config_upsert_reuses_logical_config_row() {
    let Some(fixture) = postgres_mapi_calendar_fixture().await.unwrap() else {
        return;
    };

    let first = fixture
        .storage
        .upsert_mapi_associated_config(crate::store::UpsertMapiAssociatedConfigInput {
            id: None,
            account_id: fixture.account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.RssRule".to_string(),
            subject: "IPM.Configuration.RssRule".to_string(),
            properties_json: serde_json::json!({"version": 1}),
        })
        .await
        .unwrap();
    let second = fixture
        .storage
        .upsert_mapi_associated_config(crate::store::UpsertMapiAssociatedConfigInput {
            id: None,
            account_id: fixture.account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.RssRule".to_string(),
            subject: "IPM.Configuration.RssRule".to_string(),
            properties_json: serde_json::json!({"version": 2}),
        })
        .await
        .unwrap();

    assert_eq!(second.id, first.id);
    assert_eq!(second.properties_json, serde_json::json!({"version": 2}));
    let explicit_new_id = Uuid::parse_str("10000000-0000-0000-0000-000000000015").unwrap();
    let explicit = fixture
        .storage
        .upsert_mapi_associated_config(crate::store::UpsertMapiAssociatedConfigInput {
            id: Some(explicit_new_id),
            account_id: fixture.account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.RssRule".to_string(),
            subject: "IPM.Configuration.RssRule".to_string(),
            properties_json: serde_json::json!({"version": 22}),
        })
        .await
        .unwrap();

    assert_eq!(explicit.id, first.id);
    assert_eq!(explicit.properties_json, serde_json::json!({"version": 22}));
    let configs = fixture
        .storage
        .fetch_mapi_associated_configs(fixture.account_id)
        .await
        .unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].id, first.id);

    let stale_id = Uuid::parse_str("10000000-0000-0000-0000-000000000016").unwrap();
    let tenant_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT tenant_id
        FROM accounts
        WHERE id = $1
        "#,
    )
    .bind(fixture.account_id)
    .fetch_one(fixture.storage.pool())
    .await
    .unwrap();
    sqlx::query("DROP INDEX mapi_associated_config_messages_logical_idx")
        .execute(fixture.storage.pool())
        .await
        .unwrap();
    sqlx::query(
        r#"
        INSERT INTO mapi_associated_config_messages (
            tenant_id, id, account_id, folder_id, message_class, subject, properties_json
        )
        VALUES ($1, $2, $3, $4, 'IPM.Configuration.RssRule', 'IPM.Configuration.RssRule', '{"stale":true}'::jsonb)
        "#,
    )
    .bind(tenant_id)
    .bind(stale_id)
    .bind(fixture.account_id)
    .bind(crate::mapi::identity::INBOX_FOLDER_ID as i64)
    .execute(fixture.storage.pool())
    .await
    .unwrap();

    let configs = fixture
        .storage
        .fetch_mapi_associated_configs(fixture.account_id)
        .await
        .unwrap();
    assert_eq!(
        configs
            .iter()
            .filter(|config| config.message_class == "IPM.Configuration.RssRule")
            .count(),
        1
    );

    let third = fixture
        .storage
        .upsert_mapi_associated_config(crate::store::UpsertMapiAssociatedConfigInput {
            id: None,
            account_id: fixture.account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.RssRule".to_string(),
            subject: "IPM.Configuration.RssRule".to_string(),
            properties_json: serde_json::json!({"version": 3}),
        })
        .await
        .unwrap();
    assert_eq!(third.id, stale_id);
    let physical_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mapi_associated_config_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND folder_id = $3
          AND message_class = 'IPM.Configuration.RssRule'
        "#,
    )
    .bind(tenant_id)
    .bind(fixture.account_id)
    .bind(crate::mapi::identity::INBOX_FOLDER_ID as i64)
    .fetch_one(fixture.storage.pool())
    .await
    .unwrap();
    assert_eq!(physical_count, 1);

    fixture.cleanup().await.unwrap();
}

#[tokio::test]
async fn mapi_navigation_shortcut_upsert_reuses_logical_shortcut_row() {
    let Some(fixture) = postgres_mapi_calendar_fixture().await.unwrap() else {
        return;
    };

    let first = fixture
        .storage
        .upsert_mapi_navigation_shortcut(crate::store::UpsertMapiNavigationShortcutInput {
            id: None,
            account_id: fixture.account_id,
            subject: "Inbox".to_string(),
            target_folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 127,
            group_header_id: None,
            group_name: "Mail".to_string(),
        })
        .await
        .unwrap();
    let second = fixture
        .storage
        .upsert_mapi_navigation_shortcut(crate::store::UpsertMapiNavigationShortcutInput {
            id: None,
            account_id: fixture.account_id,
            subject: "Inbox".to_string(),
            target_folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 8,
            save_stamp: 0,
            section: 1,
            ordinal: 191,
            group_header_id: None,
            group_name: "Mail".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(second.id, first.id);
    assert_eq!(second.flags, 8);
    assert_eq!(second.ordinal, 191);
    let third = fixture
        .storage
        .upsert_mapi_navigation_shortcut(crate::store::UpsertMapiNavigationShortcutInput {
            id: None,
            account_id: fixture.account_id,
            subject: "Inbox".to_string(),
            target_folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 8,
            save_stamp: 0,
            section: 1,
            ordinal: 191,
            group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
            group_name: "Mail".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(third.id, first.id);
    assert_eq!(
        fixture
            .storage
            .fetch_mapi_navigation_shortcuts(fixture.account_id)
            .await
            .unwrap()
            .len(),
        1
    );
    let stale_calendar = fixture
        .storage
        .upsert_mapi_navigation_shortcut(crate::store::UpsertMapiNavigationShortcutInput {
            id: None,
            account_id: fixture.account_id,
            subject: "Calendar".to_string(),
            target_folder_id: Some(crate::mapi::identity::CALENDAR_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 3,
            ordinal: 127,
            group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
            group_name: "My Calendars".to_string(),
        })
        .await
        .unwrap();
    let outlook_calendar_group_id =
        Uuid::parse_str("b7f00600-0000-0000-c000-000000000046").unwrap();
    let corrected_calendar = fixture
        .storage
        .upsert_mapi_navigation_shortcut(crate::store::UpsertMapiNavigationShortcutInput {
            id: None,
            account_id: fixture.account_id,
            subject: "Calendar".to_string(),
            target_folder_id: Some(crate::mapi::identity::CALENDAR_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 3,
            ordinal: 127,
            group_header_id: Some(outlook_calendar_group_id),
            group_name: "My Calendars".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(corrected_calendar.id, stale_calendar.id);
    assert_eq!(
        corrected_calendar.group_header_id,
        Some(outlook_calendar_group_id)
    );
    assert_eq!(corrected_calendar.group_name, "My Calendars");
    assert_eq!(
        fixture
            .storage
            .fetch_mapi_navigation_shortcuts(fixture.account_id)
            .await
            .unwrap()
            .len(),
        2
    );
    let shortcut_identity = fixture
        .storage
        .fetch_or_allocate_mapi_identities(
            fixture.account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::NavigationShortcut,
                canonical_id: first.id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await
        .unwrap()
        .remove(0);
    let common_views_mailbox_id =
        mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID)
            .unwrap()
            .id;
    let changed = fixture
        .storage
        .fetch_mapi_sync_changes(
            fixture.account_id,
            Some(common_views_mailbox_id),
            MapiCheckpointKind::Content,
            0,
        )
        .await
        .unwrap();
    assert!(changed.changed_navigation_shortcut_ids.contains(&first.id));

    fixture
        .storage
        .delete_mapi_navigation_shortcut(fixture.account_id, first.id)
        .await
        .unwrap();
    let deleted = fixture
        .storage
        .fetch_mapi_sync_changes(
            fixture.account_id,
            Some(common_views_mailbox_id),
            MapiCheckpointKind::Content,
            changed.current_change_sequence,
        )
        .await
        .unwrap();
    assert!(deleted.deleted_navigation_shortcut_ids.contains(&first.id));
    assert_eq!(
        fixture
            .storage
            .fetch_mapi_object_ids_for_deleted_changes(
                fixture.account_id,
                MapiIdentityObjectKind::NavigationShortcut,
                &[first.id],
            )
            .await
            .unwrap(),
        vec![shortcut_identity.object_id]
    );
    assert!(fixture
        .storage
        .fetch_mapi_identities_by_object_ids(fixture.account_id, &[shortcut_identity.object_id])
        .await
        .unwrap()
        .is_empty());

    fixture.cleanup().await.unwrap();
}

#[tokio::test]
async fn mapi_default_calendar_folder_identity_is_persisted() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![CollaborationCollection {
            id: "default".to_string(),
            kind: "calendar".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            display_name: "Calendar".to_string(),
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
        }])),
        ..FakeStore::default()
    };

    store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();

    let calendar_folder =
        mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID).unwrap();
    assert_eq!(
        store
            .mapi_identities
            .lock()
            .unwrap()
            .get(&calendar_folder.id)
            .copied(),
        Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
}

#[tokio::test]
async fn mapi_calendar_event_identity_survives_restart_style_store_reload() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("71717171-7171-7171-7171-71717171abcd").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-06-03".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Stable calendar identity".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..FakeStore::default()
    };

    let first = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();
    let second = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();
    let first_events = first.events_for_folder(crate::mapi::identity::CALENDAR_FOLDER_ID);
    let second_events = second.events_for_folder(crate::mapi::identity::CALENDAR_FOLDER_ID);
    assert_eq!(first_events.len(), 1);
    assert_eq!(second_events.len(), 1);
    let first_event = first_events[0];
    let second_event = second_events[0];

    assert_eq!(first_event.id, second_event.id);
    assert_eq!(first_event.canonical_id, event_id);
    assert_eq!(second_event.canonical_id, event_id);
    assert_eq!(
        crate::mapi::identity::object_id_from_long_term_id(
            &crate::mapi::identity::long_term_id_from_object_id(first_event.id).unwrap()
        ),
        Some(second_event.id)
    );
}

#[tokio::test]
async fn mapi_full_snapshot_loads_messages_without_search_index_query() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        fail_query_jmap_email_ids: true,
        ..FakeStore::default()
    };
    let mailbox = FakeStore::mailbox("44444444-4444-4444-4444-444444444444", "inbox", "Inbox");
    let email = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        &mailbox.id.to_string(),
        "inbox",
        "Visible without search document",
    );
    store.mailboxes.lock().unwrap().push(mailbox);
    store.emails.lock().unwrap().push(email);

    let snapshot = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();

    assert_eq!(snapshot.messages().len(), 1);
    assert_eq!(store.queried_jmap_email_ids.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn mapi_full_snapshot_does_not_persist_virtual_special_mailbox_identity() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..FakeStore::default()
    };
    let virtual_mailbox = mapi_mailstore::virtual_special_mailbox(
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    )
    .unwrap();
    store
        .mailboxes
        .lock()
        .unwrap()
        .push(virtual_mailbox.clone());

    let snapshot = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();

    assert_eq!(snapshot.folders().len(), 1);
    assert!(!store
        .mapi_identities
        .lock()
        .unwrap()
        .contains_key(&virtual_mailbox.id));
}

#[tokio::test]
async fn mapi_identity_source_key_lookup_and_checkpoints_round_trip() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..FakeStore::default()
    };
    let mailbox = FakeStore::mailbox(
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "Source key",
    );
    store.mailboxes.lock().unwrap().push(mailbox.clone());
    let allocated = store
        .fetch_or_allocate_mapi_identities(
            account.account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::Mailbox,
                canonical_id: mailbox.id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await
        .unwrap();
    let source_key = crate::mapi::identity::source_key_for_object_id(allocated[0].object_id);
    let looked_up = store
        .fetch_mapi_identities_by_source_keys(account.account_id, &[source_key])
        .await
        .unwrap();

    assert_eq!(looked_up[0].canonical_id, mailbox.id);

    let checkpoint = store
        .store_mapi_sync_checkpoint(
            account.account_id,
            Some(mailbox.id),
            MapiCheckpointKind::Content,
            42,
            7,
            serde_json::json!({"last": "message"}),
        )
        .await
        .unwrap();
    let fetched = store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(mailbox.id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(fetched, checkpoint);
}

#[tokio::test]
async fn mapi_identity_allocator_ignores_high_reserved_counters() {
    let Some(fixture) = postgres_mapi_calendar_fixture().await.unwrap() else {
        return;
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let tenant_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT tenant_id
        FROM accounts
        WHERE id = $1
        "#,
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    let high_counter = crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER - 1;
    let high_identity_id = Uuid::parse_str("21000000-0000-0000-0000-000000000001").unwrap();
    let (high_object_id, source_key, change_key, instance_key) =
        crate::mapi::identity::persisted_identity_material(high_counter);
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id, mapi_global_counter,
            mapi_object_id, source_key, change_key, instance_key
        )
        VALUES ($1, $2, 'associated_config', $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(high_identity_id)
    .bind(high_counter as i64)
    .bind(high_object_id as i64)
    .bind(source_key)
    .bind(change_key)
    .bind(instance_key)
    .execute(storage.pool())
    .await
    .unwrap();
    sqlx::query(
        r#"
        UPDATE mapi_mailbox_replicas
        SET next_global_counter = $3
        WHERE tenant_id = $1 AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind((crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 1) as i64)
    .execute(storage.pool())
    .await
    .unwrap();

    let allocated = storage
        .fetch_or_allocate_mapi_identities(
            account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::Contact,
                canonical_id: Uuid::parse_str("21000000-0000-0000-0000-000000000002").unwrap(),
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await
        .unwrap()
        .remove(0);
    let allocated_counter =
        crate::mapi::identity::global_counter_from_store_id(allocated.object_id).unwrap();
    let next_counter = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT next_global_counter
        FROM mapi_mailbox_replicas
        WHERE tenant_id = $1 AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_one(storage.pool())
    .await
    .unwrap() as u64;

    assert!(allocated_counter < crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER);
    assert!(next_counter < crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER);

    fixture.cleanup().await.unwrap();
}

#[tokio::test]
async fn mapi_identity_repair_removes_orphaned_checkpoint_and_config_state() {
    let Some(fixture) = postgres_mapi_calendar_fixture().await.unwrap() else {
        return;
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let tenant_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT tenant_id
        FROM accounts
        WHERE id = $1
        "#,
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    let mailbox = storage
        .create_jmap_mailbox(
            JmapMailboxCreateInput {
                account_id,
                name: "MAPI repair".to_string(),
                parent_id: None,
                sort_order: Some(300),
                is_subscribed: true,
            },
            lpe_storage::AuditEntryInput {
                actor: "alice@example.test".to_string(),
                action: "create-mailbox".to_string(),
                subject: account_id.to_string(),
            },
        )
        .await
        .unwrap();
    let identity = storage
        .fetch_or_allocate_mapi_identities(
            account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::Mailbox,
                canonical_id: mailbox.id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await
        .unwrap()
        .remove(0);
    storage
        .store_mapi_sync_checkpoint(
            account_id,
            Some(mailbox.id),
            MapiCheckpointKind::Content,
            12,
            3,
            serde_json::json!({"source": "valid"}),
        )
        .await
        .unwrap();
    storage
        .upsert_mapi_associated_config(crate::store::UpsertMapiAssociatedConfigInput {
            account_id,
            id: Some(Uuid::parse_str("20000000-0000-0000-0000-000000000001").unwrap()),
            folder_id: identity.object_id,
            message_class: "IPM.Configuration.Valid".to_string(),
            subject: "IPM.Configuration.Valid".to_string(),
            properties_json: serde_json::json!({}),
        })
        .await
        .unwrap();

    let missing_mailbox_id = Uuid::parse_str("20000000-0000-0000-0000-000000000002").unwrap();
    let orphaned_config_id = Uuid::parse_str("20000000-0000-0000-0000-000000000003").unwrap();
    sqlx::query(
        r#"
        INSERT INTO mapi_sync_checkpoints (
            id, tenant_id, account_id, mailbox_id, checkpoint_kind, mapi_replica_guid,
            last_change_sequence, last_modseq, cursor_json, expires_at
        )
        VALUES ($1, $2, $3, $4, 'content', $5, 50, 10, '{"source":"orphan"}'::jsonb, NOW() + INTERVAL '30 days')
        "#,
    )
    .bind(Uuid::parse_str("20000000-0000-0000-0000-000000000004").unwrap())
    .bind(tenant_id)
    .bind(account_id)
    .bind(missing_mailbox_id)
    .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
    .execute(storage.pool())
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO mapi_associated_config_messages (
            tenant_id, id, account_id, folder_id, message_class, subject, properties_json
        )
        VALUES ($1, $2, $3, 983041, 'IPM.Configuration.Orphan', 'IPM.Configuration.Orphan', '{}'::jsonb)
        "#,
    )
    .bind(tenant_id)
    .bind(orphaned_config_id)
    .bind(account_id)
    .execute(storage.pool())
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id, mapi_global_counter,
            mapi_object_id, source_key, change_key, instance_key
        )
        SELECT $1, $2, 'associated_config', $3, 50000, (50000::bigint << 16) | 1,
               decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex') || decode(lpad(to_hex(50000), 12, '0'), 'hex'),
               decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex') || decode(lpad(to_hex(50000), 12, '0'), 'hex'),
               decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex') || decode(lpad(to_hex(50000), 12, '0'), 'hex')
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(orphaned_config_id)
    .execute(storage.pool())
    .await
    .unwrap();
    let orphaned_config_object_id = ((50000i64) << 16) | 1;
    let stale_search_folder_id = Uuid::parse_str("20000000-0000-0000-0000-000000000006").unwrap();
    let stale_search_folder_object_id = ((50002i64) << 16) | 1;
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id, mapi_global_counter,
            mapi_object_id, source_key, change_key, instance_key
        )
        SELECT $1, $2, 'search_folder_definition', $3, 50002, $4,
               decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex') || decode(lpad(to_hex(50002), 12, '0'), 'hex'),
               decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex') || decode(lpad(to_hex(50002), 12, '0'), 'hex'),
               decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex') || decode(lpad(to_hex(50002), 12, '0'), 'hex')
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(stale_search_folder_id)
    .bind(stale_search_folder_object_id)
    .execute(storage.pool())
    .await
    .unwrap();
    let stale_search_folder_source_key = sqlx::query_scalar::<_, Vec<u8>>(
        r#"
        SELECT source_key
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'search_folder_definition'
          AND canonical_id = $3
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(stale_search_folder_id)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert!(storage
        .fetch_mapi_identities_by_object_ids(
            account_id,
            &[
                orphaned_config_object_id as u64,
                stale_search_folder_object_id as u64,
            ],
        )
        .await
        .unwrap()
        .is_empty());
    assert!(storage
        .fetch_mapi_identities_by_source_keys(account_id, &[stale_search_folder_source_key])
        .await
        .unwrap()
        .is_empty());
    let active_search_folder_id = Uuid::parse_str("20000000-0000-0000-0000-000000000007").unwrap();
    sqlx::query(
        r#"
        INSERT INTO search_folders (
            id, tenant_id, account_id, role, display_name, definition_kind,
            result_object_kind, scope_json, restriction_json, is_builtin
        )
        VALUES ($1, $2, $3, 'custom', 'Active search', 'user_saved',
                'message', '{}'::jsonb, '{"kind":"mapi_bounded","all":[{"kind":"exists"}]}'::jsonb, FALSE)
        "#,
    )
    .bind(active_search_folder_id)
    .bind(tenant_id)
    .bind(account_id)
    .execute(storage.pool())
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO mail_change_log (
            tenant_id, account_id, object_kind, object_id, change_kind,
            modseq, affected_principal_ids, summary_json
        )
        VALUES
            ($1, $2, 'search_folder_definition', $3, 'created', 103, ARRAY[$2]::uuid[], '{}'::jsonb),
            ($1, $2, 'search_folder_definition', $4, 'created', 104, ARRAY[$2]::uuid[], '{}'::jsonb)
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(active_search_folder_id)
    .bind(stale_search_folder_id)
    .execute(storage.pool())
    .await
    .unwrap();
    let deleted_mailbox_object_id = ((50001i64) << 16) | 1;
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id, mapi_global_counter,
            mapi_object_id, source_key, change_key, instance_key, deleted_at
        )
        SELECT $1, $2, 'mailbox', $3, 50001, $4,
               decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex') || decode(lpad(to_hex(50001), 12, '0'), 'hex'),
               decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex') || decode(lpad(to_hex(50001), 12, '0'), 'hex'),
               decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex') || decode(lpad(to_hex(50001), 12, '0'), 'hex'),
               NOW()
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(missing_mailbox_id)
    .bind(deleted_mailbox_object_id)
    .execute(storage.pool())
    .await
    .unwrap();
    let deleted_search_folder_id = Uuid::parse_str("20000000-0000-0000-0000-000000000008").unwrap();
    let deleted_search_folder_object_id = ((50003i64) << 16) | 1;
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id, mapi_global_counter,
            mapi_object_id, source_key, change_key, instance_key, deleted_at
        )
        SELECT $1, $2, 'search_folder_definition', $3, 50003, $4,
               decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex') || decode(lpad(to_hex(50003), 12, '0'), 'hex'),
               decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex') || decode(lpad(to_hex(50003), 12, '0'), 'hex'),
               decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex') || decode(lpad(to_hex(50003), 12, '0'), 'hex'),
               NOW()
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(deleted_search_folder_id)
    .bind(deleted_search_folder_object_id)
    .execute(storage.pool())
    .await
    .unwrap();
    let deleted_search_folder_change_cursor = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO mail_change_log (
            tenant_id, account_id, object_kind, object_id, change_kind,
            modseq, affected_principal_ids, summary_json
        )
        VALUES ($1, $2, 'search_folder_definition', $3, 'destroyed', 105, ARRAY[$2]::uuid[], '{}'::jsonb)
        RETURNING cursor
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(deleted_search_folder_id)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO tombstones (
            id, tenant_id, account_id, object_kind, object_id,
            deleted_modseq, change_cursor, reason
        )
        VALUES ($1, $2, $3, 'search_folder_definition', $4, 105, $5, 'destroyed')
        "#,
    )
    .bind(Uuid::parse_str("20000000-0000-0000-0000-000000000009").unwrap())
    .bind(tenant_id)
    .bind(account_id)
    .bind(deleted_search_folder_id)
    .bind(deleted_search_folder_change_cursor)
    .execute(storage.pool())
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO mail_change_log (
            tenant_id, account_id, mailbox_id, object_kind, object_id, change_kind,
            modseq, affected_principal_ids, summary_json
        )
        VALUES ($1, $2, $3, 'mailbox', $3, 'created', 100, ARRAY[$2]::uuid[], '{}'::jsonb)
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(missing_mailbox_id)
    .execute(storage.pool())
    .await
    .unwrap();
    let deleted_mailbox_change_cursor = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO mail_change_log (
            tenant_id, account_id, mailbox_id, object_kind, object_id, change_kind,
            modseq, affected_principal_ids, summary_json
        )
        VALUES ($1, $2, $3, 'mailbox', $3, 'destroyed', 102, ARRAY[$2]::uuid[], '{}'::jsonb)
        RETURNING cursor
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(missing_mailbox_id)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO tombstones (
            id, tenant_id, account_id, mailbox_id, object_kind, object_id,
            deleted_modseq, change_cursor, reason
        )
        VALUES ($1, $2, $3, $4, 'mailbox', $4, 102, $5, 'destroyed')
        "#,
    )
    .bind(Uuid::parse_str("20000000-0000-0000-0000-000000000005").unwrap())
    .bind(tenant_id)
    .bind(account_id)
    .bind(missing_mailbox_id)
    .bind(deleted_mailbox_change_cursor)
    .execute(storage.pool())
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO mail_change_log (
            tenant_id, account_id, object_kind, object_id, change_kind,
            modseq, affected_principal_ids, summary_json
        )
        VALUES ($1, $2, 'associated_config', $3, 'updated', 101, ARRAY[$2]::uuid[], '{"folderId":"983041"}'::jsonb)
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(orphaned_config_id)
    .execute(storage.pool())
    .await
    .unwrap();

    storage
        .fetch_or_allocate_mapi_identities(
            account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::Mailbox,
                canonical_id: mailbox.id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await
        .unwrap();

    let checkpoint_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mapi_sync_checkpoints
        WHERE tenant_id = $1 AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    let config_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mapi_associated_config_messages
        WHERE tenant_id = $1 AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    let orphaned_config_identity_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'associated_config'
          AND canonical_id = $3
          AND deleted_at IS NULL
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(orphaned_config_id)
    .fetch_one(storage.pool())
    .await
    .unwrap();

    assert_eq!(checkpoint_count, 1);
    assert_eq!(config_count, 1);
    assert_eq!(orphaned_config_identity_count, 0);
    assert!(storage
        .fetch_mapi_sync_checkpoint(account_id, Some(mailbox.id), MapiCheckpointKind::Content)
        .await
        .unwrap()
        .is_some());
    let associated_configs = storage
        .fetch_mapi_associated_configs(account_id)
        .await
        .unwrap();
    assert_eq!(
        associated_configs[0].id,
        Uuid::parse_str("20000000-0000-0000-0000-000000000001").unwrap()
    );
    let hierarchy_changes = storage
        .fetch_mapi_sync_changes(account_id, None, MapiCheckpointKind::Hierarchy, 0)
        .await
        .unwrap();
    let content_changes = storage
        .fetch_mapi_sync_changes(account_id, None, MapiCheckpointKind::Content, 0)
        .await
        .unwrap();
    assert!(!hierarchy_changes
        .changed_mailbox_ids
        .contains(&missing_mailbox_id));
    assert!(hierarchy_changes
        .changed_mailbox_ids
        .contains(&active_search_folder_id));
    assert!(!hierarchy_changes
        .changed_mailbox_ids
        .contains(&stale_search_folder_id));
    assert!(hierarchy_changes
        .deleted_mailbox_object_ids
        .contains(&(deleted_mailbox_object_id as u64)));
    assert!(hierarchy_changes
        .deleted_search_folder_object_ids
        .contains(&(deleted_search_folder_object_id as u64)));
    assert!(!content_changes
        .changed_associated_config_ids
        .iter()
        .any(|change| change.config_id == orphaned_config_id));
    assert!(content_changes
        .changed_associated_config_ids
        .iter()
        .any(|change| change.config_id == associated_configs[0].id));

    fixture.cleanup().await.unwrap();
}

#[derive(Clone, Default)]
struct FakeStore {
    session: Option<AuthenticatedAccount>,
    contact_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
    calendar_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
    task_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
    contacts: Arc<Mutex<Vec<AccessibleContact>>>,
    group_aliases: Arc<Mutex<Vec<(Uuid, String, String)>>>,
    group_alias_members: Arc<Mutex<HashMap<Uuid, Vec<String>>>>,
    contact_versions: Arc<Mutex<HashMap<Uuid, u64>>>,
    deleted_contacts: Arc<Mutex<Vec<Uuid>>>,
    events: Arc<Mutex<Vec<AccessibleEvent>>>,
    event_versions: Arc<Mutex<HashMap<Uuid, u64>>>,
    deleted_events: Arc<Mutex<Vec<Uuid>>>,
    tasks: Arc<Mutex<Vec<ClientTask>>>,
    task_versions: Arc<Mutex<HashMap<Uuid, u64>>>,
    deleted_tasks: Arc<Mutex<Vec<Uuid>>>,
    notes: Arc<Mutex<Vec<ClientNote>>>,
    journal_entries: Arc<Mutex<Vec<JournalEntry>>>,
    active_sieve_script: Arc<Mutex<Option<String>>>,
    mailbox_rules: Arc<Mutex<Vec<MailboxRule>>>,
    saved_drafts: Arc<Mutex<Vec<SubmitMessageInput>>>,
    imported_emails: Arc<Mutex<Vec<JmapImportedEmailInput>>>,
    emails: Arc<Mutex<Vec<JmapEmail>>>,
    public_folders: Arc<Mutex<Vec<PublicFolder>>>,
    deleted_public_folders: Arc<Mutex<Vec<Uuid>>>,
    public_folder_items: Arc<Mutex<Vec<PublicFolderItem>>>,
    public_folder_permissions: Arc<Mutex<Vec<PublicFolderPermission>>>,
    public_folder_replicas: Arc<Mutex<Vec<PublicFolderReplica>>>,
    public_folder_per_user_states: Arc<Mutex<Vec<PublicFolderPerUserState>>>,
    deleted_public_folder_items: Arc<Mutex<Vec<Uuid>>>,
    attachments: Arc<Mutex<HashMap<Uuid, Vec<ActiveSyncAttachment>>>>,
    calendar_attachments: Arc<Mutex<HashMap<Uuid, Vec<CalendarEventAttachment>>>>,
    attachment_contents: Arc<Mutex<HashMap<String, ActiveSyncAttachmentContent>>>,
    created_attachments: Arc<Mutex<Vec<AttachmentUploadInput>>>,
    submitted_messages: Arc<Mutex<Vec<SubmitMessageInput>>>,
    submitted_draft_messages: Arc<Mutex<Vec<Uuid>>>,
    cancelled_submissions: Arc<Mutex<Vec<Uuid>>>,
    submission_cancel_results: Arc<Mutex<HashMap<Uuid, CancelSubmissionResult>>>,
    deleted_emails: Arc<Mutex<Vec<Uuid>>>,
    moved_emails: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
    copied_emails: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
    failed_delete_email_ids: Arc<Mutex<Vec<Uuid>>>,
    recoverable_items: Arc<Mutex<Vec<RecoverableItem>>>,
    restored_recoverable_items: Arc<Mutex<Vec<(Uuid, Option<Uuid>)>>>,
    purged_recoverable_items: Arc<Mutex<Vec<Uuid>>>,
    failed_purge_recoverable_item_ids: Arc<Mutex<Vec<Uuid>>>,
    mailboxes: Arc<Mutex<Vec<JmapMailbox>>>,
    queried_jmap_email_ids: Arc<AtomicU64>,
    created_mailboxes: Arc<Mutex<Vec<JmapMailboxCreateInput>>>,
    updated_mailboxes: Arc<Mutex<Vec<JmapMailboxUpdateInput>>>,
    destroyed_mailboxes: Arc<Mutex<Vec<Uuid>>>,
    directory_accounts: Arc<Mutex<Vec<AuthenticatedAccount>>>,
    extra_address_book_entries: Arc<Mutex<Vec<ExchangeAddressBookEntry>>>,
    extra_address_book_entry_tenants: Arc<Mutex<HashMap<Uuid, Uuid>>>,
    hidden_address_book_entry_ids: Arc<Mutex<Vec<Uuid>>>,
    ews_im_groups: Arc<Mutex<Vec<EwsImGroup>>>,
    ews_im_group_members: Arc<Mutex<Vec<EwsImGroupMember>>>,
    mapi_identities: Arc<Mutex<HashMap<Uuid, u64>>>,
    mapi_identity_source_keys: Arc<Mutex<HashMap<Uuid, Vec<u8>>>>,
    mapi_named_properties: Arc<Mutex<FakeMapiNamedProperties>>,
    mapi_custom_property_values: Arc<Mutex<HashMap<FakeMapiCustomPropertyKey, Vec<u8>>>>,
    mapi_folder_profile_property_values:
        Arc<Mutex<HashMap<FakeMapiFolderProfilePropertyKey, Vec<u8>>>>,
    mapi_checkpoints: Arc<Mutex<HashMap<(Option<Uuid>, MapiCheckpointKind), MapiSyncCheckpoint>>>,
    stale_protocol_local_folder_properties: Arc<Mutex<HashMap<(u64, u32), Vec<u8>>>>,
    mapi_sync_changes: Arc<Mutex<MapiSyncChangeSet>>,
    mapi_folder_permissions: Arc<Mutex<Vec<MapiFolderPermission>>>,
    mapi_calendar_permissions: Arc<Mutex<Vec<MapiFolderPermission>>>,
    mapi_folder_permission_audits: Arc<Mutex<Vec<lpe_storage::AuditEntryInput>>>,
    mapi_ipm_subtree_ost_id: Arc<Mutex<Option<Vec<u8>>>>,
    fail_mapi_ipm_subtree_ost_id_store: bool,
    search_folders: Arc<Mutex<Vec<SearchFolderDefinition>>>,
    deleted_search_folders: Arc<Mutex<Vec<Uuid>>>,
    navigation_shortcuts: Arc<Mutex<Vec<crate::store::MapiNavigationShortcutRecord>>>,
    associated_configs: Arc<Mutex<Vec<crate::store::MapiAssociatedConfigRecord>>>,
    conversation_actions: Arc<Mutex<Vec<ConversationAction>>>,
    delegate_freebusy_messages: Arc<Mutex<Vec<DelegateFreeBusyMessageObject>>>,
    reminders: Arc<Mutex<Vec<ClientReminder>>>,
    mapi_notification_cursor: Arc<Mutex<Option<i64>>>,
    mapi_notification_polls: Arc<Mutex<Vec<MapiNotificationPoll>>>,
    ews_user_configurations: Arc<Mutex<Vec<EwsUserConfiguration>>>,
    ews_delegates: Arc<Mutex<Vec<EwsDelegate>>>,
    ews_retention_policy_tags: Arc<Mutex<Vec<FakeRetentionPolicyTag>>>,
    ews_sharing_grants: Arc<Mutex<Vec<CollaborationGrant>>>,
    ews_discovery_search_configs: Arc<Mutex<Vec<EwsDiscoverySearchConfig>>>,
    ews_discovery_search_results: Arc<Mutex<Vec<EwsDiscoverySearchResult>>>,
    ews_message_tracking_reports: Arc<Mutex<Vec<FakeMessageTrackingReport>>>,
    ews_message_tracking_events: Arc<Mutex<HashMap<String, Vec<EwsMessageTrackingEvent>>>>,
    ews_holds: Arc<Mutex<Vec<EwsHoldMailbox>>>,
    ews_non_indexable_reports: Arc<Mutex<Vec<EwsNonIndexableReport>>>,
    ews_transfer_jobs: Arc<Mutex<Vec<EwsTransferJob>>>,
    ews_mail_app_manifests: Arc<Mutex<Vec<FakeMailAppManifest>>>,
    ews_mail_app_installations: Arc<Mutex<Vec<FakeMailAppInstallation>>>,
    ews_mail_app_token_events: Arc<Mutex<Vec<EwsMailAppTokenEvent>>>,
    ews_app_marketplace_policy: Arc<Mutex<EwsAppMarketplacePolicy>>,
    ews_unified_messaging_calls: Arc<Mutex<Vec<FakeUnifiedMessagingCall>>>,
    next_mapi_global_counter: Arc<Mutex<u64>>,
    omit_principal_from_directory: bool,
    fail_query_jmap_email_ids: bool,
    mapi_mail_store_load_started: Option<Arc<tokio::sync::Notify>>,
    mapi_mail_store_load_continue: Option<Arc<tokio::sync::Notify>>,
}

type FakeMapiCustomPropertyKey = (Uuid, MapiCustomPropertyObjectKind, Uuid, u32, u16);
type FakeMapiFolderProfilePropertyKey = (Uuid, u64, u32, u16);

#[derive(Clone)]
struct FakeRetentionPolicyTag {
    tenant_id: Uuid,
    assigned_account_id: Option<Uuid>,
    tag: EwsRetentionPolicyTag,
}

#[derive(Clone)]
struct FakeMailAppManifest {
    tenant_id: Uuid,
    manifest: EwsMailAppManifest,
}

#[derive(Clone)]
struct FakeMailAppInstallation {
    tenant_id: Uuid,
    account_id: Uuid,
    catalog_id: Uuid,
    app_id: String,
    status: String,
}

#[derive(Clone)]
struct FakeUnifiedMessagingCall {
    tenant_id: Uuid,
    account_id: Uuid,
    call: EwsUnifiedMessagingCall,
}

#[derive(Clone)]
struct FakeMessageTrackingReport {
    tenant_id: Uuid,
    report: EwsMessageTrackingReport,
}

#[derive(Default)]
struct FakeMapiNamedProperties {
    by_property: HashMap<(Uuid, MapiNamedProperty), u16>,
    by_id: HashMap<(Uuid, u16), MapiNamedProperty>,
}

const FAKE_PS_INTERNET_HEADERS_GUID: [u8; 16] = [
    0x86, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];

fn fake_normalize_mapi_named_property(mut property: MapiNamedProperty) -> MapiNamedProperty {
    if property.guid == FAKE_PS_INTERNET_HEADERS_GUID {
        if let MapiNamedPropertyKind::Name(name) = property.kind {
            property.kind = MapiNamedPropertyKind::Name(name.to_ascii_lowercase());
        }
    }
    property
}

const PID_TAG_BODY_W: u32 = 0x1000_001F;

fn display_to_for_test(email: &JmapEmail) -> String {
    email
        .to
        .iter()
        .map(|address| {
            address
                .display_name
                .as_deref()
                .unwrap_or(&address.address)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn test_message_flags(email: &JmapEmail) -> u32 {
    let mut flags = 0u32;
    if !email.unread {
        flags |= 0x0000_0001;
    }
    if email.has_attachments {
        flags |= 0x0000_0010;
    }
    flags
}

fn fake_contact_phone_by_label(contact: &AccessibleContact, labels: &[&str]) -> String {
    fake_contact_phone_values_by_label(contact, labels)
        .into_iter()
        .next()
        .unwrap_or_default()
}

fn fake_contact_phone_values_by_label(contact: &AccessibleContact, labels: &[&str]) -> Vec<String> {
    contact
        .phones_json
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| {
            let label = item
                .get("label")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            labels
                .iter()
                .any(|expected| label.eq_ignore_ascii_case(expected))
        })
        .filter_map(|item| item.get("phone").and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn fake_contact_address_value(contact: &AccessibleContact, keys: &[&str]) -> String {
    contact
        .addresses_json
        .as_array()
        .into_iter()
        .flatten()
        .find_map(|item| {
            keys.iter()
                .filter_map(|key| item.get(*key).and_then(serde_json::Value::as_str))
                .map(str::trim)
                .find(|value| !value.is_empty())
                .map(ToString::to_string)
        })
        .unwrap_or_default()
}

fn jmap_search_matches(email: &JmapEmail, search_text: &str) -> bool {
    let needle = search_text.trim().to_ascii_lowercase();
    if needle.is_empty() {
        return true;
    }

    let contains = |value: &str| value.to_ascii_lowercase().contains(&needle);
    contains(&email.subject)
        || contains(&email.preview)
        || contains(&email.body_text)
        || contains(&email.from_address)
        || email.from_display.as_deref().is_some_and(contains)
        || email.sender_address.as_deref().is_some_and(contains)
        || email.sender_display.as_deref().is_some_and(contains)
        || email.to.iter().chain(email.cc.iter()).any(|recipient| {
            contains(&recipient.address) || recipient.display_name.as_deref().is_some_and(contains)
        })
}

#[derive(Clone)]
struct FakeDetector {
    detection: MagikaDetection,
}

impl FakeDetector {
    fn pdf() -> Self {
        Self {
            detection: MagikaDetection {
                label: "pdf".to_string(),
                mime_type: "application/pdf".to_string(),
                description: "PDF document".to_string(),
                group: "document".to_string(),
                extensions: vec!["pdf".to_string()],
                score: Some(0.99),
            },
        }
    }

    fn png() -> Self {
        Self {
            detection: MagikaDetection {
                label: "png".to_string(),
                mime_type: "image/png".to_string(),
                description: "PNG image".to_string(),
                group: "image".to_string(),
                extensions: vec!["png".to_string()],
                score: Some(0.99),
            },
        }
    }

    fn text() -> Self {
        Self {
            detection: MagikaDetection {
                label: "txt".to_string(),
                mime_type: "text/plain".to_string(),
                description: "Plain text".to_string(),
                group: "text".to_string(),
                extensions: vec!["txt".to_string()],
                score: Some(0.99),
            },
        }
    }

    fn executable() -> Self {
        Self {
            detection: MagikaDetection {
                label: "pebin".to_string(),
                mime_type: "application/vnd.microsoft.portable-executable".to_string(),
                description: "Windows executable".to_string(),
                group: "executable".to_string(),
                extensions: vec!["exe".to_string()],
                score: Some(0.99),
            },
        }
    }
}

impl Detector for FakeDetector {
    fn detect(&self, _source: DetectionSource<'_>) -> anyhow::Result<MagikaDetection> {
        Ok(self.detection.clone())
    }
}

impl FakeStore {
    fn account() -> AuthenticatedAccount {
        AuthenticatedAccount {
            tenant_id: Uuid::from_u128(0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa),
            account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            expires_at: "2099-01-01T00:00:00Z".to_string(),
        }
    }

    fn rights() -> CollaborationRights {
        CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        }
    }

    fn collection(id: &str, kind: &str, display_name: &str) -> CollaborationCollection {
        let account = Self::account();
        CollaborationCollection {
            id: id.to_string(),
            kind: kind.to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            display_name: display_name.to_string(),
            is_owned: true,
            rights: Self::rights(),
        }
    }

    fn contact(id: &str, name: &str, email: &str) -> AccessibleContact {
        let account = Self::account();
        AccessibleContact {
            id: Uuid::parse_str(id).unwrap(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            rights: Self::rights(),
            name: name.to_string(),
            role: String::new(),
            email: email.to_string(),
            phone: String::new(),
            team: String::new(),
            notes: String::new(),
            ..Default::default()
        }
    }

    fn mailbox(id: &str, role: &str, name: &str) -> JmapMailbox {
        JmapMailbox {
            id: Uuid::parse_str(id).unwrap(),
            parent_id: None,
            role: role.to_string(),
            name: name.to_string(),
            sort_order: 40,
            modseq: 40,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }
    }

    fn email(id: &str, mailbox_id: &str, mailbox_role: &str, subject: &str) -> JmapEmail {
        let account = Self::account();
        let mailbox_id = Uuid::parse_str(mailbox_id).unwrap();
        JmapEmail {
            id: Uuid::parse_str(id).unwrap(),
            thread_id: Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap(),
            mailbox_id,
            mailbox_role: mailbox_role.to_string(),
            mailbox_name: "RCA Sync".to_string(),
            modseq: 41,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: mailbox_role.to_string(),
                name: "RCA Sync".to_string(),
                modseq: 41,
                unread: false,
                flagged: false,
                followup_flag_status: "none".to_string(),
                followup_icon: 0,
                todo_item_flags: 0,
                followup_request: String::new(),
                followup_start_at: None,
                followup_due_at: None,
                followup_completed_at: None,
                reminder_set: false,
                reminder_at: None,
                reminder_dismissed_at: None,
                swapped_todo_store_id: None,
                swapped_todo_data: None,
                categories: Vec::new(),
                draft: mailbox_role == "drafts",
            }],
            received_at: "2026-05-03T12:00:00Z".to_string(),
            sent_at: None,
            from_address: account.email,
            from_display: Some(account.display_name),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: account.account_id,
            to: vec![JmapEmailAddress {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: subject.to_string(),
            preview: "Hello".to_string(),
            body_text: "Hello".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            has_attachments: false,
            size_octets: 128,
            internet_message_id: None,
            mime_blob_ref: Some(format!("test:{id}")),
            delivery_status: "stored".to_string(),
        }
    }

    fn recoverable_item(id: &str, folder: &str, subject: &str) -> RecoverableItem {
        RecoverableItem {
            id: Uuid::parse_str(id).unwrap(),
            message_id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
            source_mailbox_message_id: Uuid::parse_str("22222222-3333-4444-5555-666666666666")
                .unwrap(),
            source_mailbox_id: Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap(),
            source_imap_uid: 42,
            recoverable_folder: folder.to_string(),
            delete_kind: "hard_delete".to_string(),
            status: "active".to_string(),
            deleted_at: "2026-05-03T12:00:00Z".to_string(),
            retained_until: None,
            legal_hold: false,
            subject: subject.to_string(),
            sender_address: "alice@example.test".to_string(),
            received_at: "2026-05-03T11:00:00Z".to_string(),
            size_octets: 512,
            has_attachments: false,
        }
    }

    fn public_folder_item(id: &str, folder_id: &str, subject: &str) -> PublicFolderItem {
        let account = Self::account();
        PublicFolderItem {
            id: Uuid::parse_str(id).unwrap(),
            public_folder_id: Uuid::parse_str(folder_id).unwrap(),
            message_id: None,
            item_kind: "post".to_string(),
            message_class: "IPM.Post".to_string(),
            subject: subject.to_string(),
            body_text: "Public body".to_string(),
            body_html_sanitized: None,
            source_payload_json: "{}".to_string(),
            lifecycle_state: "active".to_string(),
            change_counter: 1,
            created_by_account_id: account.account_id,
            updated_by_account_id: account.account_id,
            is_read: false,
            created_at: "2026-05-07T12:00:00Z".to_string(),
            updated_at: "2026-05-07T12:00:00Z".to_string(),
        }
    }

    fn public_folder(id: &str, parent_id: Option<&str>, display_name: &str) -> PublicFolder {
        PublicFolder {
            id: Uuid::parse_str(id).unwrap(),
            tree_id: Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap(),
            parent_folder_id: parent_id.map(|id| Uuid::parse_str(id).unwrap()),
            canonical_id: Uuid::parse_str(id).unwrap(),
            display_name: display_name.to_string(),
            folder_class: "IPF.Note".to_string(),
            path: format!("/{display_name}"),
            sort_order: 0,
            lifecycle_state: "active".to_string(),
            change_counter: 1,
            rights: lpe_storage::PublicFolderRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
            created_at: "2026-05-07T12:00:00Z".to_string(),
            updated_at: "2026-05-07T12:00:00Z".to_string(),
        }
    }

    fn ensure_public_folder_tree_owner(account_id: Uuid) -> anyhow::Result<()> {
        if account_id == Self::account().account_id {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "public folder structural changes require tree owner access"
            ))
        }
    }

    fn public_folder_rights_for(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
    ) -> anyhow::Result<PublicFolderRights> {
        let folder = self
            .public_folders
            .lock()
            .unwrap()
            .iter()
            .find(|folder| folder.id == folder_id && folder.lifecycle_state == "active")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("public folder not found"))?;
        if account_id == Self::account().account_id {
            Ok(PublicFolderRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            })
        } else {
            Ok(folder.rights)
        }
    }

    fn ensure_public_folder_read(&self, account_id: Uuid, folder_id: Uuid) -> anyhow::Result<()> {
        if self
            .public_folder_rights_for(account_id, folder_id)?
            .may_read
        {
            Ok(())
        } else {
            Err(anyhow::anyhow!("public folder read access is not granted"))
        }
    }

    fn ensure_public_folder_write(&self, account_id: Uuid, folder_id: Uuid) -> anyhow::Result<()> {
        if self
            .public_folder_rights_for(account_id, folder_id)?
            .may_write
        {
            Ok(())
        } else {
            Err(anyhow::anyhow!("public folder write access is not granted"))
        }
    }

    fn ensure_public_folder_delete(&self, account_id: Uuid, folder_id: Uuid) -> anyhow::Result<()> {
        if self
            .public_folder_rights_for(account_id, folder_id)?
            .may_delete
        {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "public folder delete access is not granted"
            ))
        }
    }

    fn public_folder_replica(id: &str, folder_id: &str, server_name: &str) -> PublicFolderReplica {
        PublicFolderReplica {
            id: Uuid::parse_str(id).unwrap(),
            public_folder_id: Uuid::parse_str(folder_id).unwrap(),
            server_name: server_name.to_string(),
            lifecycle_state: "active".to_string(),
            sort_order: 0,
            created_at: "2026-05-07T12:00:00Z".to_string(),
            updated_at: "2026-05-07T12:00:00Z".to_string(),
        }
    }

    fn email_addresses(recipients: &[SubmittedRecipientInput]) -> Vec<JmapEmailAddress> {
        recipients
            .iter()
            .map(|recipient| JmapEmailAddress {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect()
    }

    fn fake_mapi_identity_lookup_for_object_id(
        &self,
        object_id: u64,
    ) -> Option<MapiIdentityLookupRecord> {
        let identities = self.mapi_identities.lock().unwrap().clone();
        let source_keys = self.mapi_identity_source_keys.lock().unwrap().clone();
        let mailbox_match = self
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|mailbox| {
                identities.get(&mailbox.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&mailbox.id) == object_id
                    || crate::mapi_store::reserved_folder_counter_for_role(&mailbox.role)
                        .map(crate::mapi::identity::mapi_store_id)
                        == Some(object_id)
            })
            .map(|mailbox| (MapiIdentityObjectKind::Mailbox, mailbox.id));
        let message_match = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .find(|email| {
                identities.get(&email.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&email.id) == object_id
            })
            .map(|email| (MapiIdentityObjectKind::Message, email.id));
        let contact_match = self
            .contacts
            .lock()
            .unwrap()
            .iter()
            .find(|contact| {
                identities.get(&contact.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&contact.id) == object_id
            })
            .map(|contact| (MapiIdentityObjectKind::Contact, contact.id));
        let event_match = self
            .events
            .lock()
            .unwrap()
            .iter()
            .find(|event| {
                identities.get(&event.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&event.id) == object_id
            })
            .map(|event| (MapiIdentityObjectKind::CalendarEvent, event.id));
        let task_match = self
            .tasks
            .lock()
            .unwrap()
            .iter()
            .find(|task| {
                identities.get(&task.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&task.id) == object_id
            })
            .map(|task| (MapiIdentityObjectKind::Task, task.id));
        let note_match = self
            .notes
            .lock()
            .unwrap()
            .iter()
            .find(|note| {
                identities.get(&note.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&note.id) == object_id
            })
            .map(|note| (MapiIdentityObjectKind::Note, note.id));
        let journal_entry_match = self
            .journal_entries
            .lock()
            .unwrap()
            .iter()
            .find(|entry| {
                identities.get(&entry.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&entry.id) == object_id
            })
            .map(|entry| (MapiIdentityObjectKind::JournalEntry, entry.id));
        let rule_match = self
            .mailbox_rules
            .lock()
            .unwrap()
            .iter()
            .find(|rule| {
                identities.get(&rule.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&rule.id) == object_id
            })
            .map(|rule| (MapiIdentityObjectKind::Rule, rule.id));
        let account_match = self
            .directory_accounts
            .lock()
            .unwrap()
            .iter()
            .find(|account| identities.get(&account.account_id).copied() == Some(object_id))
            .map(|account| (MapiIdentityObjectKind::Account, account.account_id));
        let public_folder_match = self
            .public_folders
            .lock()
            .unwrap()
            .iter()
            .find(|folder| identities.get(&folder.id).copied() == Some(object_id))
            .map(|folder| (MapiIdentityObjectKind::PublicFolder, folder.id));
        let associated_config_match = self
            .associated_configs
            .lock()
            .unwrap()
            .iter()
            .find(|config| identities.get(&config.id).copied() == Some(object_id))
            .map(|config| (MapiIdentityObjectKind::AssociatedConfig, config.id));
        let navigation_shortcut_match = self
            .navigation_shortcuts
            .lock()
            .unwrap()
            .iter()
            .find(|shortcut| identities.get(&shortcut.id).copied() == Some(object_id))
            .map(|shortcut| (MapiIdentityObjectKind::NavigationShortcut, shortcut.id));
        let search_folder_match = self
            .search_folders
            .lock()
            .unwrap()
            .iter()
            .find(|folder| identities.get(&folder.id).copied() == Some(object_id))
            .map(|folder| (MapiIdentityObjectKind::SearchFolderDefinition, folder.id));

        let fallback_identity_match = identities
            .iter()
            .find(|(_, mapped_object_id)| **mapped_object_id == object_id)
            .map(|(canonical_id, _)| (MapiIdentityObjectKind::AssociatedConfig, *canonical_id));

        let (object_kind, canonical_id) = mailbox_match
            .or(message_match)
            .or(contact_match)
            .or(event_match)
            .or(task_match)
            .or(note_match)
            .or(journal_entry_match)
            .or(rule_match)
            .or(account_match)
            .or(public_folder_match)
            .or(associated_config_match)
            .or(navigation_shortcut_match)
            .or(search_folder_match)
            .or(fallback_identity_match)?;
        Some(MapiIdentityLookupRecord {
            object_kind,
            canonical_id,
            object_id,
            source_key: source_keys
                .get(&canonical_id)
                .cloned()
                .unwrap_or_else(|| crate::mapi::identity::source_key_for_object_id(object_id)),
        })
    }

    fn task(id: &str, task_list_id: &str, title: &str) -> ClientTask {
        let account = Self::account();
        ClientTask {
            id: Uuid::parse_str(id).unwrap(),
            owner_account_id: account.account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            is_owned: true,
            rights: Self::rights(),
            task_list_id: Uuid::parse_str(task_list_id).unwrap(),
            task_list_sort_order: 0,
            title: title.to_string(),
            description: "Task body".to_string(),
            status: "needs-action".to_string(),
            due_at: Some("2026-05-05T09:00:00Z".to_string()),
            completed_at: None,
            recurrence_rule: String::new(),
            sort_order: 10,
            updated_at: "2026-05-04T08:00:00Z".to_string(),
        }
    }
}

impl AccountAuthStore for FakeStore {
    fn fetch_account_session<'a>(
        &'a self,
        token: &'a str,
    ) -> StoreFuture<'a, Option<AuthenticatedAccount>> {
        let session = (token == "token").then(|| self.session.clone()).flatten();
        Box::pin(async move { Ok(session) })
    }

    fn fetch_account_login<'a>(&'a self, _email: &'a str) -> StoreFuture<'a, Option<AccountLogin>> {
        Box::pin(async move { Ok(None) })
    }

    fn fetch_active_account_app_passwords<'a>(
        &'a self,
        _email: &'a str,
    ) -> StoreFuture<'a, Vec<StoredAccountAppPassword>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn touch_account_app_password<'a>(
        &'a self,
        _email: &'a str,
        _app_password_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    fn append_audit_event<'a>(
        &'a self,
        _tenant_id: &'a Uuid,
        _entry: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

impl ExchangeStore for FakeStore {
    fn fetch_ews_user_configuration<'a>(
        &'a self,
        account_id: Uuid,
        key: &'a EwsUserConfigurationKey,
    ) -> StoreFuture<'a, Option<EwsUserConfiguration>> {
        let configuration = self
            .ews_user_configurations
            .lock()
            .unwrap()
            .iter()
            .find(|configuration| {
                account_id == FakeStore::account().account_id
                    && configuration.scope_kind == key.scope_kind
                    && configuration.mailbox_id == key.mailbox_id
                    && configuration.public_folder_id == key.public_folder_id
                    && configuration.config_name == key.config_name
                    && configuration.config_class == key.config_class
            })
            .cloned();
        Box::pin(async move { Ok(configuration) })
    }

    fn upsert_ews_user_configuration<'a>(
        &'a self,
        input: UpsertEwsUserConfigurationInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, EwsUserConfiguration> {
        let mut configurations = self.ews_user_configurations.lock().unwrap();
        if let Some(configuration) = configurations.iter_mut().find(|configuration| {
            configuration.scope_kind == input.key.scope_kind
                && configuration.mailbox_id == input.key.mailbox_id
                && configuration.public_folder_id == input.key.public_folder_id
                && configuration.config_name == input.key.config_name
                && configuration.config_class == input.key.config_class
        }) {
            configuration.dictionary_json = input.dictionary_json;
            configuration.xml_payload = input.xml_payload;
            configuration.binary_payload = input.binary_payload;
            configuration.modseq += 1;
            let configuration = configuration.clone();
            return Box::pin(async move { Ok(configuration) });
        }
        let configuration = EwsUserConfiguration {
            id: Uuid::new_v4(),
            scope_kind: input.key.scope_kind,
            mailbox_id: input.key.mailbox_id,
            public_folder_id: input.key.public_folder_id,
            config_name: input.key.config_name,
            config_class: input.key.config_class,
            dictionary_json: input.dictionary_json,
            xml_payload: input.xml_payload,
            binary_payload: input.binary_payload,
            modseq: 1,
        };
        configurations.push(configuration.clone());
        Box::pin(async move { Ok(configuration) })
    }

    fn delete_ews_user_configuration<'a>(
        &'a self,
        account_id: Uuid,
        key: &'a EwsUserConfigurationKey,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, bool> {
        let deleted = if account_id == FakeStore::account().account_id {
            let mut configurations = self.ews_user_configurations.lock().unwrap();
            let before = configurations.len();
            configurations.retain(|configuration| {
                !(configuration.scope_kind == key.scope_kind
                    && configuration.mailbox_id == key.mailbox_id
                    && configuration.public_folder_id == key.public_folder_id
                    && configuration.config_name == key.config_name
                    && configuration.config_class == key.config_class)
            });
            configurations.len() != before
        } else {
            false
        };
        Box::pin(async move { Ok(deleted) })
    }

    fn fetch_ews_retention_policy_tags<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsRetentionPolicyTag>> {
        let tags = self
            .ews_retention_policy_tags
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| entry.tenant_id == principal.tenant_id)
            .filter(|entry| {
                entry.tag.is_visible || entry.assigned_account_id == Some(principal.account_id)
            })
            .map(|entry| {
                let mut tag = entry.tag.clone();
                tag.opted_into = entry.assigned_account_id == Some(principal.account_id);
                tag
            })
            .collect();
        Box::pin(async move { Ok(tags) })
    }

    fn create_managed_retention_folder<'a>(
        &'a self,
        input: ManagedRetentionFolderCreateInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        let tenant_id = self
            .session
            .as_ref()
            .map(|account| account.tenant_id)
            .unwrap_or_else(|| Self::account().tenant_id);
        let tag = self
            .ews_retention_policy_tags
            .lock()
            .unwrap()
            .iter()
            .find(|entry| {
                entry.tenant_id == tenant_id
                    && entry
                        .tag
                        .display_name
                        .eq_ignore_ascii_case(&input.folder_name)
                    && (entry.tag.tag_type == "custom_folder" || entry.tag.tag_type == "personal")
                    && (entry.tag.is_visible || entry.assigned_account_id == Some(input.account_id))
            })
            .map(|entry| entry.tag.clone());
        let result = tag
            .map(|tag| {
                let mailbox_id = Uuid::from_u128(
                    0x44444444_4444_4444_4444_444444445000
                        + self.mailboxes.lock().unwrap().len() as u128,
                );
                let mailbox = JmapMailbox {
                    id: mailbox_id,
                    parent_id: None,
                    role: "custom".to_string(),
                    name: tag.display_name,
                    sort_order: 40,
                    modseq: 40,
                    total_emails: 0,
                    unread_emails: 0,
                    size_octets: 0,
                    is_subscribed: input.is_subscribed,
                };
                self.mailboxes.lock().unwrap().push(mailbox.clone());
                self.created_mailboxes
                    .lock()
                    .unwrap()
                    .push(JmapMailboxCreateInput {
                        account_id: input.account_id,
                        name: input.folder_name,
                        parent_id: None,
                        sort_order: None,
                        is_subscribed: input.is_subscribed,
                    });
                Ok(mailbox)
            })
            .unwrap_or_else(|| Err(anyhow::anyhow!("managed retention folder tag not found")));
        Box::pin(async move { result })
    }

    fn fetch_ews_searchable_mailboxes<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsSearchableMailbox>> {
        let mut accounts = self.directory_accounts.lock().unwrap().clone();
        accounts.push(AuthenticatedAccount {
            tenant_id: principal.tenant_id,
            account_id: principal.account_id,
            email: principal.email.clone(),
            display_name: principal.display_name.clone(),
            expires_at: "2099-01-01T00:00:00Z".to_string(),
        });
        let mut mailboxes = accounts
            .into_iter()
            .filter(|account| account.tenant_id == principal.tenant_id)
            .map(|account| EwsSearchableMailbox {
                account_id: account.account_id,
                email: account.email,
                display_name: account.display_name,
                litigation_hold_enabled: false,
            })
            .collect::<Vec<_>>();
        mailboxes.sort_by(|a, b| a.email.cmp(&b.email));
        mailboxes.dedup_by(|a, b| a.account_id == b.account_id);
        Box::pin(async move { Ok(mailboxes) })
    }

    fn fetch_ews_discovery_search_configurations<'a>(
        &'a self,
        _principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsDiscoverySearchConfig>> {
        let configs = self.ews_discovery_search_configs.lock().unwrap().clone();
        Box::pin(async move { Ok(configs) })
    }

    fn search_ews_mailboxes<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        query_text: &'a str,
        mailbox_emails: &'a [String],
        limit: usize,
    ) -> StoreFuture<'a, EwsDiscoverySearchResult> {
        let scoped_emails = if mailbox_emails.is_empty() {
            vec![principal.email.to_ascii_lowercase()]
        } else {
            mailbox_emails
                .iter()
                .map(|value| value.trim().to_ascii_lowercase())
                .collect()
        };
        let mut accounts = self.directory_accounts.lock().unwrap().clone();
        accounts.push(AuthenticatedAccount {
            tenant_id: principal.tenant_id,
            account_id: principal.account_id,
            email: principal.email.clone(),
            display_name: principal.display_name.clone(),
            expires_at: "2099-01-01T00:00:00Z".to_string(),
        });
        let accounts = accounts
            .into_iter()
            .filter(|account| account.tenant_id == principal.tenant_id)
            .map(|account| EwsSearchableMailbox {
                account_id: account.account_id,
                email: account.email.to_ascii_lowercase(),
                display_name: account.display_name,
                litigation_hold_enabled: false,
            })
            .collect::<Vec<_>>();
        let scoped_account_id = accounts
            .iter()
            .find(|account| scoped_emails.iter().any(|email| email == &account.email))
            .map(|account| account.account_id)
            .unwrap_or(principal.account_id);
        let query = query_text.trim().to_ascii_lowercase();
        let emails = self.emails.lock().unwrap().clone();
        let sink = self.ews_discovery_search_results.clone();
        Box::pin(async move {
            let mut items = emails
                .into_iter()
                .filter(|email| {
                    query.is_empty()
                        || email.subject.to_ascii_lowercase().contains(&query)
                        || email.body_text.to_ascii_lowercase().contains(&query)
                        || email.preview.to_ascii_lowercase().contains(&query)
                })
                .take(limit.max(1).min(100))
                .enumerate()
                .map(|(rank, email)| EwsDiscoverySearchItem {
                    id: Uuid::new_v4(),
                    account_id: scoped_account_id,
                    mailbox_message_id: email.id,
                    message_id: email.id,
                    subject: email.subject,
                    preview: email.preview,
                    rank: rank as i32,
                })
                .collect::<Vec<_>>();
            let result = EwsDiscoverySearchResult {
                search_id: Uuid::new_v4(),
                job_id: Uuid::new_v4(),
                query_text: query_text.trim().to_string(),
                result_count: items.len(),
                items: std::mem::take(&mut items),
            };
            sink.lock().unwrap().push(result.clone());
            Ok(result)
        })
    }

    fn fetch_ews_message_tracking_reports<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        query_text: &'a str,
        limit: usize,
    ) -> StoreFuture<'a, Vec<EwsMessageTrackingReport>> {
        let query = query_text.trim().to_ascii_lowercase();
        let mut reports = self
            .ews_message_tracking_reports
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| entry.tenant_id == principal.tenant_id)
            .filter(|entry| entry.report.account_id == principal.account_id)
            .filter(|entry| {
                query.is_empty()
                    || entry.report.report_id.to_ascii_lowercase().contains(&query)
                    || entry
                        .report
                        .trace_id
                        .as_deref()
                        .unwrap_or_default()
                        .to_ascii_lowercase()
                        .contains(&query)
                    || entry.report.subject.to_ascii_lowercase().contains(&query)
                    || entry.report.sender.to_ascii_lowercase().contains(&query)
                    || entry
                        .report
                        .remote_message_ref
                        .as_deref()
                        .unwrap_or_default()
                        .to_ascii_lowercase()
                        .contains(&query)
                    || entry
                        .report
                        .recipients
                        .iter()
                        .any(|recipient| recipient.to_ascii_lowercase().contains(&query))
            })
            .map(|entry| entry.report.clone())
            .collect::<Vec<_>>();
        reports.sort_by(|a, b| b.submitted_at.cmp(&a.submitted_at));
        reports.truncate(limit.max(1).min(100));
        Box::pin(async move { Ok(reports) })
    }

    fn fetch_ews_message_tracking_report_detail<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        report_id: &'a str,
    ) -> StoreFuture<'a, Option<EwsMessageTrackingReportDetail>> {
        let report_id = report_id.trim().to_string();
        let report = self
            .ews_message_tracking_reports
            .lock()
            .unwrap()
            .iter()
            .find(|entry| {
                entry.tenant_id == principal.tenant_id
                    && entry.report.account_id == principal.account_id
                    && (entry.report.report_id == report_id
                        || entry.report.trace_id.as_deref() == Some(report_id.as_str()))
            })
            .map(|entry| entry.report.clone());
        let events = report.as_ref().map(|report| {
            self.ews_message_tracking_events
                .lock()
                .unwrap()
                .get(&report.report_id)
                .cloned()
                .unwrap_or_default()
        });
        Box::pin(async move {
            Ok(report.map(|report| EwsMessageTrackingReportDetail {
                report,
                events: events.unwrap_or_default(),
            }))
        })
    }

    fn fetch_ews_hold_mailboxes<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        mailbox_emails: &'a [String],
    ) -> StoreFuture<'a, Vec<EwsHoldMailbox>> {
        let scoped_emails = if mailbox_emails.is_empty() {
            vec![principal.email.to_ascii_lowercase()]
        } else {
            mailbox_emails
                .iter()
                .map(|value| value.trim().to_ascii_lowercase())
                .collect()
        };
        let holds = self
            .ews_holds
            .lock()
            .unwrap()
            .iter()
            .filter(|hold| scoped_emails.iter().any(|email| email == &hold.email))
            .cloned()
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(holds) })
    }

    fn set_ews_hold_mailboxes<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        hold_name: &'a str,
        query_text: &'a str,
        mailbox_emails: &'a [String],
        enable: bool,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, Vec<EwsHoldMailbox>> {
        let scoped_emails = if mailbox_emails.is_empty() {
            vec![principal.email.to_ascii_lowercase()]
        } else {
            mailbox_emails
                .iter()
                .map(|value| value.trim().to_ascii_lowercase())
                .collect()
        };
        let mut accounts = self.directory_accounts.lock().unwrap().clone();
        accounts.push(AuthenticatedAccount {
            tenant_id: principal.tenant_id,
            account_id: principal.account_id,
            email: principal.email.clone(),
            display_name: principal.display_name.clone(),
            expires_at: "2099-01-01T00:00:00Z".to_string(),
        });
        let accounts = accounts
            .into_iter()
            .filter(|account| account.tenant_id == principal.tenant_id)
            .map(|account| EwsSearchableMailbox {
                account_id: account.account_id,
                email: account.email.to_ascii_lowercase(),
                display_name: account.display_name,
                litigation_hold_enabled: false,
            })
            .collect::<Vec<_>>();
        let holds = self.ews_holds.clone();
        let hold_name = hold_name.trim().to_string();
        let query_text = query_text.trim().to_string();
        Box::pin(async move {
            let mut guard = holds.lock().unwrap();
            if enable {
                for account in accounts
                    .into_iter()
                    .filter(|account| scoped_emails.iter().any(|email| email == &account.email))
                {
                    guard.push(EwsHoldMailbox {
                        account_id: account.account_id,
                        email: account.email,
                        display_name: account.display_name,
                        hold_id: Some(Uuid::new_v4()),
                        hold_name: Some(if hold_name.is_empty() {
                            "EWS Litigation Hold".to_string()
                        } else {
                            hold_name.clone()
                        }),
                        query_text: Some(query_text.clone()),
                        active: true,
                    });
                }
            } else {
                for hold in guard.iter_mut() {
                    if scoped_emails.iter().any(|email| email == &hold.email)
                        && hold
                            .hold_name
                            .as_deref()
                            .is_some_and(|name| name == hold_name)
                    {
                        hold.active = false;
                    }
                }
            }
            Ok(guard
                .iter()
                .filter(|hold| scoped_emails.iter().any(|email| email == &hold.email))
                .cloned()
                .collect())
        })
    }

    fn fetch_ews_non_indexable_reports<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsNonIndexableReport>> {
        let reports = self
            .ews_non_indexable_reports
            .lock()
            .unwrap()
            .iter()
            .filter(|report| {
                report.account_id == principal.account_id
                    || self
                        .directory_accounts
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|account| {
                            account.tenant_id == principal.tenant_id
                                && account.account_id == report.account_id
                        })
            })
            .cloned()
            .collect();
        Box::pin(async move { Ok(reports) })
    }

    fn create_ews_transfer_job<'a>(
        &'a self,
        _principal: &'a AccountPrincipal,
        direction: &'a str,
        item_ids: &'a [String],
        _request_json: serde_json::Value,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, EwsTransferJob> {
        let jobs = self.ews_transfer_jobs.clone();
        let direction = direction.to_string();
        let item_ids = item_ids.to_vec();
        Box::pin(async move {
            let entries = item_ids
                .iter()
                .enumerate()
                .map(|(ordinal, item_id)| EwsTransferEntry {
                    id: Uuid::new_v4(),
                    ordinal: ordinal as i32,
                    item_kind: "message".to_string(),
                    canonical_id: item_id
                        .trim()
                        .strip_prefix("message:")
                        .unwrap_or_else(|| item_id.trim())
                        .parse()
                        .ok(),
                    source_item_id: Some(item_id.clone()),
                    status: "pending".to_string(),
                })
                .collect::<Vec<_>>();
            let job = EwsTransferJob {
                id: Uuid::new_v4(),
                direction,
                status: "requested".to_string(),
                total_items: entries.len(),
                entries,
            };
            jobs.lock().unwrap().push(job.clone());
            Ok(job)
        })
    }

    fn fetch_ews_mail_app_manifests<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsMailAppManifest>> {
        let installations = self.ews_mail_app_installations.lock().unwrap().clone();
        let mut manifests = self
            .ews_mail_app_manifests
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| entry.tenant_id == principal.tenant_id)
            .map(|entry| {
                let mut manifest = entry.manifest.clone();
                manifest.installation_status = installations
                    .iter()
                    .find(|install| {
                        install.tenant_id == principal.tenant_id
                            && install.account_id == principal.account_id
                            && install.catalog_id == manifest.catalog_id
                            && install.status != "uninstalled"
                    })
                    .map(|install| install.status.clone());
                manifest
            })
            .collect::<Vec<_>>();
        manifests.sort_by(|a, b| a.display_name.cmp(&b.display_name));
        Box::pin(async move { Ok(manifests) })
    }

    fn fetch_ews_app_marketplace_policy<'a>(
        &'a self,
        _principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, EwsAppMarketplacePolicy> {
        let policy = self.ews_app_marketplace_policy.lock().unwrap().clone();
        Box::pin(async move { Ok(policy) })
    }

    fn install_ews_mail_app<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        app_id: &'a str,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, EwsMailAppInstall> {
        let Some(manifest) = self
            .ews_mail_app_manifests
            .lock()
            .unwrap()
            .iter()
            .find(|entry| entry.tenant_id == principal.tenant_id && entry.manifest.app_id == app_id)
            .map(|entry| entry.manifest.clone())
        else {
            return Box::pin(async move { Err(anyhow::anyhow!("mail app not found")) });
        };
        let mut installs = self.ews_mail_app_installations.lock().unwrap();
        if let Some(install) = installs.iter_mut().find(|install| {
            install.tenant_id == principal.tenant_id
                && install.account_id == principal.account_id
                && install.catalog_id == manifest.catalog_id
                && install.status != "uninstalled"
        }) {
            install.status = "installed".to_string();
        } else {
            installs.push(FakeMailAppInstallation {
                tenant_id: principal.tenant_id,
                account_id: principal.account_id,
                catalog_id: manifest.catalog_id,
                app_id: app_id.to_string(),
                status: "installed".to_string(),
            });
        }
        let install = EwsMailAppInstall {
            catalog_id: manifest.catalog_id,
            app_id: app_id.to_string(),
            status: "installed".to_string(),
        };
        Box::pin(async move { Ok(install) })
    }

    fn disable_ews_mail_app<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        app_id: &'a str,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, EwsMailAppInstall> {
        let mut installs = self.ews_mail_app_installations.lock().unwrap();
        let updated = installs.iter_mut().find(|install| {
            install.tenant_id == principal.tenant_id
                && install.account_id == principal.account_id
                && install.app_id == app_id
                && install.status != "uninstalled"
        });
        let Some(install) = updated else {
            return Box::pin(
                async move { Err(anyhow::anyhow!("mail app installation not found")) },
            );
        };
        install.status = "disabled".to_string();
        let install = EwsMailAppInstall {
            catalog_id: install.catalog_id,
            app_id: install.app_id.clone(),
            status: install.status.clone(),
        };
        Box::pin(async move { Ok(install) })
    }

    fn uninstall_ews_mail_app<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        app_id: &'a str,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, EwsMailAppInstall> {
        let mut installs = self.ews_mail_app_installations.lock().unwrap();
        let updated = installs.iter_mut().find(|install| {
            install.tenant_id == principal.tenant_id
                && install.account_id == principal.account_id
                && install.app_id == app_id
                && install.status != "uninstalled"
        });
        let Some(install) = updated else {
            return Box::pin(
                async move { Err(anyhow::anyhow!("mail app installation not found")) },
            );
        };
        install.status = "uninstalled".to_string();
        let catalog_id = install.catalog_id;
        let install = EwsMailAppInstall {
            catalog_id,
            app_id: install.app_id.clone(),
            status: install.status.clone(),
        };
        self.ews_mail_app_token_events
            .lock()
            .unwrap()
            .retain(|event| event.catalog_id != catalog_id);
        Box::pin(async move { Ok(install) })
    }

    fn issue_ews_mail_app_token<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        app_id: &'a str,
        _token_hash: &'a str,
        _scopes: &'a [String],
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, EwsMailAppTokenEvent> {
        let installed = self
            .ews_mail_app_installations
            .lock()
            .unwrap()
            .iter()
            .find(|install| {
                install.tenant_id == principal.tenant_id
                    && install.account_id == principal.account_id
                    && install.app_id == app_id
                    && install.status == "installed"
            })
            .cloned();
        let Some(install) = installed else {
            return Box::pin(async move {
                Err(anyhow::anyhow!(
                    "mail app token access is not granted for an installed app"
                ))
            });
        };
        let event = EwsMailAppTokenEvent {
            id: Uuid::new_v4(),
            catalog_id: install.catalog_id,
            app_id: app_id.to_string(),
            issued_at: "2026-05-30T12:00:00Z".to_string(),
            expires_at: "2026-05-30T12:05:00Z".to_string(),
        };
        self.ews_mail_app_token_events
            .lock()
            .unwrap()
            .push(event.clone());
        Box::pin(async move { Ok(event) })
    }

    fn create_ews_unified_messaging_call<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        phone_number: Option<&'a str>,
        message_id: Option<Uuid>,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, EwsUnifiedMessagingCall> {
        let mut calls = self.ews_unified_messaging_calls.lock().unwrap();
        let call = EwsUnifiedMessagingCall {
            id: Uuid::new_v4(),
            call_id: format!("ews-call-{}", calls.len() + 1),
            call_kind: "play_on_phone".to_string(),
            status: "requested".to_string(),
            phone_number: phone_number.map(ToString::to_string),
            message_id,
            requested_at: "2026-05-30T12:00:00Z".to_string(),
            updated_at: "2026-05-30T12:00:00Z".to_string(),
        };
        calls.push(FakeUnifiedMessagingCall {
            tenant_id: principal.tenant_id,
            account_id: principal.account_id,
            call: call.clone(),
        });
        Box::pin(async move { Ok(call) })
    }

    fn fetch_ews_unified_messaging_call<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        call_id: &'a str,
    ) -> StoreFuture<'a, Option<EwsUnifiedMessagingCall>> {
        let call = self
            .ews_unified_messaging_calls
            .lock()
            .unwrap()
            .iter()
            .find(|entry| {
                entry.tenant_id == principal.tenant_id
                    && entry.account_id == principal.account_id
                    && entry.call.call_id == call_id
            })
            .map(|entry| entry.call.clone());
        Box::pin(async move { Ok(call) })
    }

    fn disconnect_ews_unified_messaging_call<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        call_id: &'a str,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, Option<EwsUnifiedMessagingCall>> {
        let call = self
            .ews_unified_messaging_calls
            .lock()
            .unwrap()
            .iter_mut()
            .find(|entry| {
                entry.tenant_id == principal.tenant_id
                    && entry.account_id == principal.account_id
                    && entry.call.call_id == call_id
                    && entry.call.status == "requested"
            })
            .map(|entry| {
                entry.call.status = "cancelled".to_string();
                entry.call.updated_at = "2026-05-30T12:01:00Z".to_string();
                entry.call.clone()
            });
        Box::pin(async move { Ok(call) })
    }

    fn upsert_ews_sharing_grant<'a>(
        &'a self,
        owner_account_id: Uuid,
        grantee_email: &'a str,
        kind: CollaborationResourceKind,
        rights: CollaborationRights,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, CollaborationGrant> {
        let principal = self.session.clone().unwrap_or_else(FakeStore::account);
        let owner = self
            .directory_accounts
            .lock()
            .unwrap()
            .iter()
            .find(|account| {
                account.tenant_id == principal.tenant_id && account.account_id == owner_account_id
            })
            .cloned()
            .or_else(|| (principal.account_id == owner_account_id).then_some(principal.clone()));
        let grantee = self
            .directory_accounts
            .lock()
            .unwrap()
            .iter()
            .find(|account| {
                account.tenant_id == principal.tenant_id
                    && account.email.eq_ignore_ascii_case(grantee_email)
            })
            .cloned()
            .or_else(|| {
                principal
                    .email
                    .eq_ignore_ascii_case(grantee_email)
                    .then_some(principal.clone())
            });
        let grants = self.ews_sharing_grants.clone();
        Box::pin(async move {
            let Some(owner) = owner else {
                anyhow::bail!("sharing owner account not found in tenant")
            };
            let Some(grantee) = grantee else {
                anyhow::bail!("sharing grantee account not found in tenant")
            };
            let grant = CollaborationGrant {
                id: Uuid::new_v4(),
                kind: kind.as_str().to_string(),
                calendar_id: None,
                calendar_name: None,
                owner_account_id,
                owner_email: owner.email,
                owner_display_name: owner.display_name,
                grantee_account_id: grantee.account_id,
                grantee_email: grantee.email,
                grantee_display_name: grantee.display_name,
                rights,
                created_at: "2026-05-22T00:00:00Z".to_string(),
                updated_at: "2026-05-22T00:00:00Z".to_string(),
            };
            let mut grants = grants.lock().unwrap();
            grants.retain(|existing| {
                !(existing.kind == grant.kind
                    && existing.owner_account_id == grant.owner_account_id
                    && existing.grantee_account_id == grant.grantee_account_id)
            });
            grants.push(grant.clone());
            Ok(grant)
        })
    }

    fn fetch_ews_delegates<'a>(
        &'a self,
        owner_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<EwsDelegate>> {
        let delegates = self
            .ews_delegates
            .lock()
            .unwrap()
            .iter()
            .filter(|delegate| delegate.owner_account_id == owner_account_id)
            .cloned()
            .collect();
        Box::pin(async move { Ok(delegates) })
    }

    fn upsert_ews_delegate<'a>(
        &'a self,
        input: UpsertEwsDelegateInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, EwsDelegate> {
        let principal = self.session.clone().unwrap_or_else(FakeStore::account);
        let grantee = self
            .directory_accounts
            .lock()
            .unwrap()
            .iter()
            .find(|account| {
                account.tenant_id == principal.tenant_id
                    && account.email.eq_ignore_ascii_case(&input.grantee_email)
            })
            .cloned();
        Box::pin(async move {
            let Some(grantee) = grantee else {
                anyhow::bail!("delegate account not found in tenant")
            };
            let delegate = EwsDelegate {
                owner_account_id: input.owner_account_id,
                grantee_account_id: grantee.account_id,
                grantee_email: grantee.email.to_ascii_lowercase(),
                grantee_display_name: grantee.display_name,
                inbox_rights: input.inbox_rights,
                calendar_rights: input.calendar_rights,
                may_send_on_behalf: input.may_send_on_behalf,
                may_send_as: false,
                preferences: input.preferences,
            };
            let mut delegates = self.ews_delegates.lock().unwrap();
            delegates.retain(|existing| {
                !(existing.owner_account_id == delegate.owner_account_id
                    && existing.grantee_account_id == delegate.grantee_account_id)
            });
            delegates.push(delegate.clone());
            Ok(delegate)
        })
    }

    fn remove_ews_delegate<'a>(
        &'a self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, bool> {
        let mut delegates = self.ews_delegates.lock().unwrap();
        let before = delegates.len();
        delegates.retain(|delegate| {
            !(delegate.owner_account_id == owner_account_id
                && delegate.grantee_account_id == grantee_account_id)
        });
        let deleted = delegates.len() != before;
        Box::pin(async move { Ok(deleted) })
    }

    fn fetch_or_allocate_mapi_identities<'a>(
        &'a self,
        _account_id: Uuid,
        requests: &'a [MapiIdentityRequest],
    ) -> StoreFuture<'a, Vec<MapiIdentityRecord>> {
        Box::pin(async move {
            let mut identities = self.mapi_identities.lock().unwrap();
            let mut source_keys = self.mapi_identity_source_keys.lock().unwrap();
            let mut next_counter = self.next_mapi_global_counter.lock().unwrap();
            if *next_counter < crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER {
                *next_counter = crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER;
            }
            let mut records = Vec::with_capacity(requests.len());
            for request in requests {
                let object_id = if let Some(existing) = identities.get(&request.canonical_id) {
                    *existing
                } else {
                    let counter = request.reserved_global_counter.unwrap_or_else(|| {
                        if request.object_kind == MapiIdentityObjectKind::Account {
                            let value = *next_counter;
                            *next_counter = next_counter.saturating_add(1);
                            value
                        } else {
                            crate::mapi::identity::global_counter_from_store_id(
                                crate::mapi::identity::legacy_migration_object_id(
                                    &request.canonical_id,
                                ),
                            )
                            .unwrap_or_else(|| {
                                let value = *next_counter;
                                *next_counter = next_counter.saturating_add(1);
                                value
                            })
                        }
                    });
                    let object_id = crate::mapi::identity::mapi_store_id(counter);
                    if request.reserved_global_counter.is_some()
                        && counter > crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER
                    {
                        return Err(anyhow::anyhow!(
                            "reserved MAPI global counter out of range: {counter}"
                        ));
                    }
                    if request.reserved_global_counter.is_some()
                        && identities.values().any(|existing| *existing == object_id)
                    {
                        return Err(anyhow::anyhow!(
                            "reserved MAPI object id already allocated: {object_id:#018x}"
                        ));
                    }
                    identities.insert(request.canonical_id, object_id);
                    if let Some(source_key) = request.source_key.clone() {
                        source_keys.insert(request.canonical_id, source_key);
                    }
                    object_id
                };
                let source_key = request
                    .source_key
                    .clone()
                    .or_else(|| source_keys.get(&request.canonical_id).cloned())
                    .unwrap_or_else(|| crate::mapi::identity::source_key_for_object_id(object_id));
                records.push(MapiIdentityRecord {
                    canonical_id: request.canonical_id,
                    object_id,
                    source_key,
                });
            }
            Ok(records)
        })
    }

    fn fetch_public_folder_trees<'a>(
        &'a self,
        _principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderTree>> {
        let trees = self
            .public_folders
            .lock()
            .unwrap()
            .iter()
            .filter(|folder| {
                folder.parent_folder_id.is_none() && folder.lifecycle_state == "active"
            })
            .map(|folder| PublicFolderTree {
                id: folder.tree_id,
                canonical_id: folder.tree_id,
                display_name: folder.display_name.clone(),
                lifecycle_state: "active".to_string(),
                admin_owner_account_id: FakeStore::account().account_id,
                root_folder_id: Some(folder.id),
                created_at: folder.created_at.clone(),
                updated_at: folder.updated_at.clone(),
            })
            .collect();
        Box::pin(async move { Ok(trees) })
    }

    fn fetch_public_folder<'a>(
        &'a self,
        _principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, PublicFolder> {
        let folder = self
            .public_folders
            .lock()
            .unwrap()
            .iter()
            .find(|folder| folder.id == folder_id && folder.lifecycle_state == "active")
            .cloned();
        Box::pin(async move { folder.ok_or_else(|| anyhow::anyhow!("public folder not found")) })
    }

    fn fetch_public_folder_children<'a>(
        &'a self,
        _principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolder>> {
        let folders = self
            .public_folders
            .lock()
            .unwrap()
            .iter()
            .filter(|folder| {
                folder.parent_folder_id == Some(folder_id) && folder.lifecycle_state == "active"
            })
            .cloned()
            .collect();
        Box::pin(async move { Ok(folders) })
    }

    fn create_public_folder_child<'a>(
        &'a self,
        input: CreatePublicFolderInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolder> {
        let display_name = input.display_name.trim().to_string();
        if display_name.is_empty() {
            return Box::pin(async move {
                Err(anyhow::anyhow!("public folder display name is required"))
            });
        }
        let parent_exists = self.public_folders.lock().unwrap().iter().any(|folder| {
            folder.id == input.parent_folder_id && folder.lifecycle_state == "active"
        });
        if !parent_exists {
            return Box::pin(async move { Err(anyhow::anyhow!("public folder not found")) });
        }
        if let Err(error) = Self::ensure_public_folder_tree_owner(input.account_id) {
            return Box::pin(async move { Err(error) });
        }
        let folder_id = Uuid::parse_str("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd").unwrap();
        let mut folder = FakeStore::public_folder(
            &folder_id.to_string(),
            Some(&input.parent_folder_id.to_string()),
            &display_name,
        );
        folder.folder_class = input.folder_class;
        folder.sort_order = input.sort_order;
        self.public_folders.lock().unwrap().push(folder.clone());
        Box::pin(async move { Ok(folder) })
    }

    fn update_public_folder<'a>(
        &'a self,
        input: UpdatePublicFolderInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolder> {
        if let Err(error) = Self::ensure_public_folder_tree_owner(input.account_id) {
            return Box::pin(async move { Err(error) });
        }
        let updated = {
            let mut folders = self.public_folders.lock().unwrap();
            folders
                .iter_mut()
                .find(|folder| folder.id == input.folder_id && folder.lifecycle_state == "active")
                .map(|folder| {
                    if let Some(parent_folder_id) = input.parent_folder_id {
                        folder.parent_folder_id = Some(parent_folder_id);
                    }
                    if let Some(display_name) = input.display_name.clone() {
                        folder.display_name = display_name;
                    }
                    if let Some(folder_class) = input.folder_class.clone() {
                        folder.folder_class = folder_class;
                    }
                    if let Some(sort_order) = input.sort_order {
                        folder.sort_order = sort_order;
                    }
                    folder.change_counter += 1;
                    folder.clone()
                })
        };
        Box::pin(async move { updated.ok_or_else(|| anyhow::anyhow!("public folder not found")) })
    }

    fn delete_public_folder<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        let deleted = {
            let mut folders = self.public_folders.lock().unwrap();
            let has_children = folders.iter().any(|folder| {
                folder.parent_folder_id == Some(folder_id) && folder.lifecycle_state == "active"
            });
            let has_items =
                self.public_folder_items.lock().unwrap().iter().any(|item| {
                    item.public_folder_id == folder_id && item.lifecycle_state == "active"
                });
            if let Some(folder) = folders
                .iter_mut()
                .find(|folder| folder.id == folder_id && folder.lifecycle_state == "active")
            {
                if let Err(error) = Self::ensure_public_folder_tree_owner(principal_account_id) {
                    return Box::pin(async move { Err(error) });
                }
                if folder.parent_folder_id.is_none() {
                    return Box::pin(async move {
                        Err(anyhow::anyhow!("public folder tree root cannot be deleted"))
                    });
                }
                if has_children {
                    return Box::pin(async move {
                        Err(anyhow::anyhow!(
                            "public folder with active children cannot be deleted"
                        ))
                    });
                }
                if has_items {
                    return Box::pin(async move {
                        Err(anyhow::anyhow!(
                            "public folder with active items cannot be deleted"
                        ))
                    });
                }
                folder.lifecycle_state = "deleted".to_string();
                true
            } else {
                false
            }
        };
        if deleted {
            self.deleted_public_folders.lock().unwrap().push(folder_id);
            Box::pin(async move { Ok(()) })
        } else {
            Box::pin(async move { Err(anyhow::anyhow!("public folder not found")) })
        }
    }

    fn fetch_public_folder_items<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderItem>> {
        if let Err(error) = self.ensure_public_folder_read(principal_account_id, folder_id) {
            return Box::pin(async move { Err(error) });
        }
        let items: Vec<PublicFolderItem> = self
            .public_folder_items
            .lock()
            .unwrap()
            .iter()
            .filter(|item| item.public_folder_id == folder_id && item.lifecycle_state == "active")
            .cloned()
            .collect();
        Box::pin(async move { Ok(items) })
    }

    fn fetch_public_folder_items_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        item_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<PublicFolderItem>> {
        let items: Vec<PublicFolderItem> = self
            .public_folder_items
            .lock()
            .unwrap()
            .iter()
            .filter(|item| item_ids.contains(&item.id) && item.lifecycle_state == "active")
            .cloned()
            .collect();
        for item in &items {
            if let Err(error) =
                self.ensure_public_folder_read(principal_account_id, item.public_folder_id)
            {
                return Box::pin(async move { Err(error) });
            }
        }
        Box::pin(async move { Ok(items) })
    }

    fn fetch_public_folder_permissions<'a>(
        &'a self,
        _principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderPermission>> {
        let permissions = self
            .public_folder_permissions
            .lock()
            .unwrap()
            .iter()
            .filter(|permission| permission.public_folder_id == folder_id)
            .cloned()
            .collect();
        Box::pin(async move { Ok(permissions) })
    }

    fn fetch_public_folder_replicas<'a>(
        &'a self,
        _principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderReplica>> {
        let replicas = self
            .public_folder_replicas
            .lock()
            .unwrap()
            .iter()
            .filter(|replica| {
                replica.public_folder_id == folder_id && replica.lifecycle_state == "active"
            })
            .cloned()
            .collect();
        Box::pin(async move { Ok(replicas) })
    }

    fn upsert_public_folder_permission<'a>(
        &'a self,
        input: PublicFolderPermissionInput,
        audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolderPermission> {
        if !input.may_read && (input.may_write || input.may_delete || input.may_share) {
            return Box::pin(async move {
                Err(anyhow::anyhow!(
                    "read access is required when granting write, delete, or share"
                ))
            });
        }
        if input.may_delete && !input.may_write {
            return Box::pin(
                async move { Err(anyhow::anyhow!("delete access requires write access")) },
            );
        }
        if input.may_share && !input.may_write {
            return Box::pin(
                async move { Err(anyhow::anyhow!("share access requires write access")) },
            );
        }
        let Some(principal) = self
            .directory_accounts
            .lock()
            .unwrap()
            .iter()
            .find(|account| account.account_id == input.principal_account_id)
            .cloned()
        else {
            return Box::pin(async move {
                Err(anyhow::anyhow!(
                    "public folder permission principal not found"
                ))
            });
        };
        let mut permissions = self.public_folder_permissions.lock().unwrap();
        if let Some(permission) = permissions.iter_mut().find(|permission| {
            permission.public_folder_id == input.public_folder_id
                && permission.principal_account_id == input.principal_account_id
        }) {
            permission.rights = PublicFolderRights {
                may_read: input.may_read,
                may_write: input.may_write,
                may_delete: input.may_delete,
                may_share: input.may_share,
            };
            permission.updated_at = "2026-05-07T12:00:00Z".to_string();
            let permission = permission.clone();
            self.mapi_folder_permission_audits
                .lock()
                .unwrap()
                .push(audit);
            return Box::pin(async move { Ok(permission) });
        }
        let permission = PublicFolderPermission {
            id: Uuid::parse_str("dededede-dede-dede-dede-dededededede").unwrap(),
            public_folder_id: input.public_folder_id,
            principal_account_id: input.principal_account_id,
            principal_email: principal.email,
            principal_display_name: principal.display_name,
            rights: PublicFolderRights {
                may_read: input.may_read,
                may_write: input.may_write,
                may_delete: input.may_delete,
                may_share: input.may_share,
            },
            created_at: "2026-05-07T12:00:00Z".to_string(),
            updated_at: "2026-05-07T12:00:00Z".to_string(),
        };
        permissions.push(permission.clone());
        self.mapi_folder_permission_audits
            .lock()
            .unwrap()
            .push(audit);
        Box::pin(async move { Ok(permission) })
    }

    fn delete_public_folder_permission<'a>(
        &'a self,
        _principal_account_id: Uuid,
        folder_id: Uuid,
        grantee_account_id: Uuid,
        audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        let mut permissions = self.public_folder_permissions.lock().unwrap();
        let before = permissions.len();
        permissions.retain(|permission| {
            !(permission.public_folder_id == folder_id
                && permission.principal_account_id == grantee_account_id)
        });
        if permissions.len() == before {
            return Box::pin(
                async move { Err(anyhow::anyhow!("public folder permission not found")) },
            );
        }
        self.mapi_folder_permission_audits
            .lock()
            .unwrap()
            .push(audit);
        Box::pin(async move { Ok(()) })
    }

    fn upsert_public_folder_item<'a>(
        &'a self,
        input: UpsertPublicFolderItemInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolderItem> {
        if let Err(error) =
            self.ensure_public_folder_write(input.account_id, input.public_folder_id)
        {
            return Box::pin(async move { Err(error) });
        }
        let mut items = self.public_folder_items.lock().unwrap();
        let item_id = input
            .id
            .unwrap_or_else(|| Uuid::parse_str("efefefef-efef-efef-efef-efefefefefef").unwrap());
        if let Some(item) = items.iter_mut().find(|item| {
            item.id == item_id
                && item.public_folder_id == input.public_folder_id
                && item.lifecycle_state == "active"
        }) {
            item.subject = input.subject;
            item.body_text = input.body_text;
            item.body_html_sanitized = input.body_html_sanitized;
            item.message_class = input.message_class;
            item.item_kind = input.item_kind;
            item.source_payload_json = input.source_payload_json;
            item.updated_by_account_id = input.account_id;
            item.change_counter += 1;
            let item = item.clone();
            return Box::pin(async move { Ok(item) });
        }
        if input.id.is_some() {
            return Box::pin(async move { Err(anyhow::anyhow!("public folder item not found")) });
        }
        let item = PublicFolderItem {
            id: item_id,
            public_folder_id: input.public_folder_id,
            message_id: None,
            item_kind: input.item_kind,
            message_class: input.message_class,
            subject: input.subject,
            body_text: input.body_text,
            body_html_sanitized: input.body_html_sanitized,
            source_payload_json: input.source_payload_json,
            lifecycle_state: "active".to_string(),
            change_counter: 1,
            created_by_account_id: input.account_id,
            updated_by_account_id: input.account_id,
            is_read: false,
            created_at: "2026-05-07T12:00:00Z".to_string(),
            updated_at: "2026-05-07T12:00:00Z".to_string(),
        };
        items.push(item.clone());
        Box::pin(async move { Ok(item) })
    }

    fn delete_public_folder_item<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        item_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        if let Err(error) = self.ensure_public_folder_delete(principal_account_id, folder_id) {
            return Box::pin(async move { Err(error) });
        }
        let deleted = {
            let mut items = self.public_folder_items.lock().unwrap();
            if let Some(item) = items.iter_mut().find(|item| {
                item.id == item_id
                    && item.public_folder_id == folder_id
                    && item.lifecycle_state == "active"
            }) {
                item.lifecycle_state = "deleted".to_string();
                true
            } else {
                false
            }
        };
        if deleted {
            self.deleted_public_folder_items
                .lock()
                .unwrap()
                .push(item_id);
            Box::pin(async move { Ok(()) })
        } else {
            Box::pin(async move { Err(anyhow::anyhow!("public folder item not found")) })
        }
    }

    fn fetch_public_folder_per_user_state<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderPerUserState>> {
        let items = self.public_folder_items.lock().unwrap();
        let states = self.public_folder_per_user_states.lock().unwrap();
        let states = states
            .iter()
            .filter(|state| {
                state.public_folder_id == folder_id
                    && state.account_id == principal_account_id
                    && items.iter().any(|item| {
                        item.public_folder_id == folder_id
                            && item.id == state.item_id
                            && item.lifecycle_state == "active"
                    })
            })
            .cloned()
            .collect();
        Box::pin(async move { Ok(states) })
    }

    fn patch_public_folder_per_user_state<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        patches: &'a [PublicFolderPerUserStatePatch],
    ) -> StoreFuture<'a, Vec<PublicFolderPerUserState>> {
        let mut items = self.public_folder_items.lock().unwrap();
        let mut stored_states = self.public_folder_per_user_states.lock().unwrap();
        let mut states = Vec::new();
        for patch in patches {
            let Some(item) = items.iter_mut().find(|item| {
                item.public_folder_id == folder_id
                    && item.id == patch.item_id
                    && item.lifecycle_state == "active"
            }) else {
                return Box::pin(
                    async move { Err(anyhow::anyhow!("public folder item not found")) },
                );
            };
            item.is_read = patch.is_read;
            let state = PublicFolderPerUserState {
                public_folder_id: folder_id,
                item_id: patch.item_id,
                account_id: principal_account_id,
                is_read: patch.is_read,
                last_seen_change: patch.last_seen_change.unwrap_or(item.change_counter),
                private_json: patch
                    .private_json
                    .clone()
                    .unwrap_or_else(|| "{}".to_string()),
                updated_at: "2026-05-07T12:00:00Z".to_string(),
            };
            if let Some(stored) = stored_states.iter_mut().find(|stored| {
                stored.public_folder_id == folder_id
                    && stored.item_id == patch.item_id
                    && stored.account_id == principal_account_id
            }) {
                *stored = state.clone();
            } else {
                stored_states.push(state.clone());
            }
            states.push(state);
        }
        Box::pin(async move { Ok(states) })
    }

    fn fetch_mapi_identities_by_object_ids<'a>(
        &'a self,
        _account_id: Uuid,
        object_ids: &'a [u64],
    ) -> StoreFuture<'a, Vec<MapiIdentityLookupRecord>> {
        let records = object_ids
            .iter()
            .filter_map(|object_id| self.fake_mapi_identity_lookup_for_object_id(*object_id))
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(records) })
    }

    fn fetch_mapi_object_ids_for_deleted_changes<'a>(
        &'a self,
        _account_id: Uuid,
        object_kind: MapiIdentityObjectKind,
        canonical_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<u64>> {
        let identities = self.mapi_identities.lock().unwrap().clone();
        let records = canonical_ids
            .iter()
            .filter_map(|canonical_id| {
                identities.get(canonical_id).copied().filter(|object_id| {
                    self.fake_mapi_identity_lookup_for_object_id(*object_id)
                        .is_some_and(|record| record.object_kind == object_kind)
                })
            })
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(records) })
    }

    fn fetch_mapi_identities_by_source_keys<'a>(
        &'a self,
        _account_id: Uuid,
        source_keys: &'a [Vec<u8>],
    ) -> StoreFuture<'a, Vec<MapiIdentityLookupRecord>> {
        let stored_source_keys = self.mapi_identity_source_keys.lock().unwrap().clone();
        let stored_identities = self.mapi_identities.lock().unwrap().clone();
        let records = source_keys
            .iter()
            .filter_map(|source_key| {
                stored_source_keys
                    .iter()
                    .find(|(_, stored_source_key)| stored_source_key.as_slice() == source_key)
                    .and_then(|(canonical_id, _)| stored_identities.get(canonical_id).copied())
                    .or_else(|| crate::mapi::identity::object_id_from_source_key(source_key))
                    .and_then(|object_id| self.fake_mapi_identity_lookup_for_object_id(object_id))
            })
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(records) })
    }

    fn fetch_or_allocate_mapi_named_property_ids<'a>(
        &'a self,
        account_id: Uuid,
        properties: &'a [MapiNamedProperty],
        create: bool,
    ) -> StoreFuture<'a, Vec<Option<MapiNamedPropertyMapping>>> {
        Box::pin(async move {
            let mut store = self.mapi_named_properties.lock().unwrap();
            let mut mappings = Vec::with_capacity(properties.len());
            for property in properties {
                let property = fake_normalize_mapi_named_property(property.clone());
                if let Some(property_id) = store.by_property.get(&(account_id, property.clone())) {
                    mappings.push(Some(MapiNamedPropertyMapping {
                        property_id: *property_id,
                        property,
                    }));
                    continue;
                }
                if !create {
                    mappings.push(None);
                    continue;
                }

                let mut property_id = crate::mapi::properties::DYNAMIC_NAMED_PROPERTY_ID_START;
                while (store.by_id.contains_key(&(account_id, property_id))
                    || crate::mapi::properties::is_reserved_named_property_id(property_id))
                    && property_id < MAX_NAMED_PROPERTY_ID
                {
                    property_id = property_id.saturating_add(1);
                }
                if property_id > MAX_NAMED_PROPERTY_ID
                    || store.by_id.contains_key(&(account_id, property_id))
                    || crate::mapi::properties::is_reserved_named_property_id(property_id)
                {
                    return Err(anyhow::anyhow!("MAPI named property id space exhausted"));
                }
                store
                    .by_property
                    .insert((account_id, property.clone()), property_id);
                store
                    .by_id
                    .insert((account_id, property_id), property.clone());
                mappings.push(Some(MapiNamedPropertyMapping {
                    property_id,
                    property,
                }));
            }
            Ok(mappings)
        })
    }

    fn fetch_mapi_named_properties_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        property_ids: &'a [u16],
    ) -> StoreFuture<'a, Vec<MapiNamedPropertyMapping>> {
        Box::pin(async move {
            let store = self.mapi_named_properties.lock().unwrap();
            Ok(property_ids
                .iter()
                .filter_map(|property_id| {
                    store
                        .by_id
                        .get(&(account_id, *property_id))
                        .map(|property| MapiNamedPropertyMapping {
                            property_id: *property_id,
                            property: property.clone(),
                        })
                })
                .collect())
        })
    }

    fn fetch_mapi_named_properties<'a>(
        &'a self,
        account_id: Uuid,
        guid: Option<[u8; 16]>,
    ) -> StoreFuture<'a, Vec<MapiNamedPropertyMapping>> {
        Box::pin(async move {
            let store = self.mapi_named_properties.lock().unwrap();
            let mut mappings = store
                .by_id
                .iter()
                .filter_map(|((mapped_account_id, property_id), property)| {
                    if *mapped_account_id != account_id
                        || guid.is_some_and(|guid| property.guid != guid)
                    {
                        return None;
                    }
                    Some(MapiNamedPropertyMapping {
                        property_id: *property_id,
                        property: property.clone(),
                    })
                })
                .collect::<Vec<_>>();
            mappings.sort_by_key(|mapping| mapping.property_id);
            Ok(mappings)
        })
    }

    fn upsert_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
        values: &'a [MapiCustomPropertyValue],
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let mut stored = self.mapi_custom_property_values.lock().unwrap();
            for value in values {
                stored.insert(
                    (
                        account_id,
                        object_kind,
                        canonical_id,
                        value.property_tag,
                        value.property_type,
                    ),
                    value.property_value.clone(),
                );
            }
            Ok(())
        })
    }

    fn fetch_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
        property_tags: &'a [u32],
    ) -> StoreFuture<'a, Vec<MapiCustomPropertyValue>> {
        Box::pin(async move {
            let mut values = self
                .mapi_custom_property_values
                .lock()
                .unwrap()
                .iter()
                .filter_map(
                    |(
                        (
                            stored_account_id,
                            stored_object_kind,
                            stored_canonical_id,
                            property_tag,
                            property_type,
                        ),
                        property_value,
                    )| {
                        if *stored_account_id == account_id
                            && *stored_object_kind == object_kind
                            && *stored_canonical_id == canonical_id
                            && property_tags.contains(property_tag)
                        {
                            Some(MapiCustomPropertyValue {
                                property_tag: *property_tag,
                                property_type: *property_type,
                                property_value: property_value.clone(),
                            })
                        } else {
                            None
                        }
                    },
                )
                .collect::<Vec<_>>();
            values.sort_by_key(|value| value.property_tag);
            Ok(values)
        })
    }

    fn fetch_all_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
    ) -> StoreFuture<'a, Vec<MapiCustomPropertyValue>> {
        Box::pin(async move {
            let mut values = self
                .mapi_custom_property_values
                .lock()
                .unwrap()
                .iter()
                .filter_map(
                    |(
                        (
                            stored_account_id,
                            stored_object_kind,
                            stored_canonical_id,
                            property_tag,
                            property_type,
                        ),
                        property_value,
                    )| {
                        if *stored_account_id == account_id
                            && *stored_object_kind == object_kind
                            && *stored_canonical_id == canonical_id
                        {
                            Some(MapiCustomPropertyValue {
                                property_tag: *property_tag,
                                property_type: *property_type,
                                property_value: property_value.clone(),
                            })
                        } else {
                            None
                        }
                    },
                )
                .collect::<Vec<_>>();
            values.sort_by_key(|value| (value.property_tag, value.property_type));
            Ok(values)
        })
    }

    fn delete_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
        property_tags: &'a [u32],
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.mapi_custom_property_values.lock().unwrap().retain(
                |(
                    stored_account_id,
                    stored_object_kind,
                    stored_canonical_id,
                    property_tag,
                    _property_type,
                ),
                 _property_value| {
                    !(*stored_account_id == account_id
                        && *stored_object_kind == object_kind
                        && *stored_canonical_id == canonical_id
                        && property_tags.contains(property_tag))
                },
            );
            Ok(())
        })
    }

    fn fetch_mapi_sync_checkpoint<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
    ) -> StoreFuture<'a, Option<MapiSyncCheckpoint>> {
        let checkpoint = self
            .mapi_checkpoints
            .lock()
            .unwrap()
            .get(&(mailbox_id, checkpoint_kind))
            .cloned();
        Box::pin(async move { Ok(checkpoint) })
    }

    fn store_mapi_sync_checkpoint<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
        last_change_sequence: u64,
        last_modseq: u64,
        cursor_json: serde_json::Value,
    ) -> StoreFuture<'a, MapiSyncCheckpoint> {
        let checkpoint = MapiSyncCheckpoint {
            mailbox_id,
            checkpoint_kind,
            last_change_sequence,
            last_modseq,
            cursor_json,
        };
        let checkpoint = {
            let mut checkpoints = self.mapi_checkpoints.lock().unwrap();
            match checkpoints.get(&(mailbox_id, checkpoint_kind)) {
                Some(existing)
                    if existing.last_change_sequence > last_change_sequence
                        || (existing.last_change_sequence == last_change_sequence
                            && existing.last_modseq > last_modseq) =>
                {
                    existing.clone()
                }
                _ => {
                    checkpoints.insert((mailbox_id, checkpoint_kind), checkpoint.clone());
                    checkpoint
                }
            }
        };
        Box::pin(async move { Ok(checkpoint) })
    }

    fn fetch_mapi_ipm_subtree_ost_id<'a>(
        &'a self,
        _account_id: Uuid,
    ) -> StoreFuture<'a, Option<Vec<u8>>> {
        let value = self.mapi_ipm_subtree_ost_id.lock().unwrap().clone();
        Box::pin(async move { Ok(value) })
    }

    fn store_mapi_ipm_subtree_ost_id<'a>(
        &'a self,
        _account_id: Uuid,
        ost_id: &'a [u8],
    ) -> StoreFuture<'a, ()> {
        if self.fail_mapi_ipm_subtree_ost_id_store {
            return Box::pin(async move { anyhow::bail!("simulated OST identity store failure") });
        }
        *self.mapi_ipm_subtree_ost_id.lock().unwrap() = Some(ost_id.to_vec());
        Box::pin(async move { Ok(()) })
    }

    fn fetch_mapi_folder_profile_property_values<'a>(
        &'a self,
        account_id: Uuid,
        folder_id: u64,
        property_tags: &'a [u32],
    ) -> StoreFuture<'a, Vec<MapiFolderProfilePropertyValue>> {
        let values = self
            .mapi_folder_profile_property_values
            .lock()
            .unwrap()
            .iter()
            .filter_map(
                |((stored_account_id, stored_folder_id, property_tag, property_type), value)| {
                    if *stored_account_id == account_id
                        && *stored_folder_id == folder_id
                        && property_tags.contains(property_tag)
                    {
                        Some(MapiFolderProfilePropertyValue {
                            folder_id,
                            property_tag: *property_tag,
                            property_type: *property_type,
                            property_value: value.clone(),
                        })
                    } else {
                        None
                    }
                },
            )
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(values) })
    }

    fn upsert_mapi_folder_profile_property_values<'a>(
        &'a self,
        account_id: Uuid,
        values: &'a [MapiFolderProfilePropertyValue],
    ) -> StoreFuture<'a, ()> {
        let mut stored = self.mapi_folder_profile_property_values.lock().unwrap();
        for value in values {
            stored.insert(
                (
                    account_id,
                    value.folder_id,
                    value.property_tag,
                    value.property_type,
                ),
                value.property_value.clone(),
            );
        }
        Box::pin(async move { Ok(()) })
    }

    fn fetch_mapi_sync_changes<'a>(
        &'a self,
        _account_id: Uuid,
        _mailbox_id: Option<Uuid>,
        _checkpoint_kind: MapiCheckpointKind,
        _after_change_sequence: u64,
    ) -> StoreFuture<'a, MapiSyncChangeSet> {
        let changes = self.mapi_sync_changes.lock().unwrap().clone();
        Box::pin(async move { Ok(changes) })
    }

    fn fetch_mapi_folder_permissions<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<MapiFolderPermission>> {
        let mut permissions = self.mapi_folder_permissions.lock().unwrap().clone();
        if permissions.is_empty() {
            let principal = self.session.clone().unwrap_or_else(FakeStore::account);
            permissions.extend(mailbox_ids.iter().copied().map(|mailbox_id| {
                crate::mapi::permissions::owner_permission(
                    mailbox_id,
                    &AccountPrincipal {
                        tenant_id: principal.tenant_id,
                        account_id,
                        email: principal.email.clone(),
                        display_name: principal.display_name.clone(),
                        quota_mb: None,
                        quota_used_octets: None,
                    },
                )
            }));
        }
        Box::pin(async move { Ok(permissions) })
    }

    fn set_mapi_folder_permission<'a>(
        &'a self,
        owner_account_id: Uuid,
        mailbox_id: Uuid,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        let principal = self.session.clone().unwrap_or_else(FakeStore::account);
        let grantee = self
            .directory_accounts
            .lock()
            .unwrap()
            .iter()
            .find(|account| account.account_id == grantee_account_id)
            .cloned();
        Box::pin(async move {
            let Some(grantee) = grantee else {
                anyhow::bail!("grantee account not found")
            };
            let mut permissions = self.mapi_folder_permissions.lock().unwrap();
            permissions.retain(|permission| {
                !(permission.mailbox_id == mailbox_id
                    && permission.member_account_id == Some(grantee_account_id))
            });
            if may_read {
                permissions.push(MapiFolderPermission {
                    mailbox_id,
                    member_account_id: Some(grantee_account_id),
                    member_name: grantee.display_name,
                    rights: crate::mapi::permissions::rights_from_grant(
                        may_read, may_write, may_delete, may_share,
                    ),
                });
            }
            if !permissions.iter().any(|permission| {
                permission.mailbox_id == mailbox_id
                    && permission.member_account_id == Some(owner_account_id)
            }) {
                permissions.push(crate::mapi::permissions::owner_permission(
                    mailbox_id,
                    &AccountPrincipal {
                        tenant_id: principal.tenant_id,
                        account_id: owner_account_id,
                        email: principal.email,
                        display_name: principal.display_name,
                        quota_mb: None,
                        quota_used_octets: None,
                    },
                ));
            }
            self.mapi_folder_permission_audits
                .lock()
                .unwrap()
                .push(audit);
            Ok(())
        })
    }

    fn set_mapi_calendar_permission<'a>(
        &'a self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        let principal = self.session.clone().unwrap_or_else(FakeStore::account);
        let grantee = self
            .directory_accounts
            .lock()
            .unwrap()
            .iter()
            .find(|account| account.account_id == grantee_account_id)
            .cloned();
        Box::pin(async move {
            let Some(grantee) = grantee else {
                anyhow::bail!("calendar grantee account not found")
            };
            let mut permissions = self.mapi_calendar_permissions.lock().unwrap();
            permissions
                .retain(|permission| permission.member_account_id != Some(grantee_account_id));
            if may_read {
                permissions.push(MapiFolderPermission {
                    mailbox_id: Uuid::nil(),
                    member_account_id: Some(grantee_account_id),
                    member_name: grantee.display_name,
                    rights: crate::mapi::permissions::rights_from_grant(
                        may_read, may_write, may_delete, may_share,
                    ),
                });
            }
            if !permissions
                .iter()
                .any(|permission| permission.member_account_id == Some(owner_account_id))
            {
                permissions.push(crate::mapi::permissions::owner_permission(
                    Uuid::nil(),
                    &AccountPrincipal {
                        tenant_id: principal.tenant_id,
                        account_id: owner_account_id,
                        email: principal.email,
                        display_name: principal.display_name,
                        quota_mb: None,
                        quota_used_octets: None,
                    },
                ));
            }
            self.mapi_folder_permission_audits
                .lock()
                .unwrap()
                .push(audit);
            Ok(())
        })
    }

    fn set_mapi_calendar_collection_permission<'a>(
        &'a self,
        owner_account_id: Uuid,
        calendar_collection_id: &'a str,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        let principal = self.session.clone().unwrap_or_else(FakeStore::account);
        let grantee = self
            .directory_accounts
            .lock()
            .unwrap()
            .iter()
            .find(|account| account.account_id == grantee_account_id)
            .cloned();
        Box::pin(async move {
            let Some(grantee) = grantee else {
                anyhow::bail!("calendar grantee account not found")
            };
            let calendar_id = Uuid::parse_str(calendar_collection_id)?;
            let mut permissions = self.mapi_calendar_permissions.lock().unwrap();
            permissions.retain(|permission| {
                permission.mailbox_id != calendar_id
                    || permission.member_account_id != Some(grantee_account_id)
            });
            if may_read {
                permissions.push(MapiFolderPermission {
                    mailbox_id: calendar_id,
                    member_account_id: Some(grantee_account_id),
                    member_name: grantee.display_name,
                    rights: crate::mapi::permissions::rights_from_grant(
                        may_read, may_write, may_delete, may_share,
                    ),
                });
            }
            if !permissions.iter().any(|permission| {
                permission.mailbox_id == calendar_id
                    && permission.member_account_id == Some(owner_account_id)
            }) {
                permissions.push(crate::mapi::permissions::owner_permission(
                    calendar_id,
                    &AccountPrincipal {
                        tenant_id: principal.tenant_id,
                        account_id: owner_account_id,
                        email: principal.email,
                        display_name: principal.display_name,
                        quota_mb: None,
                        quota_used_octets: None,
                    },
                ));
            }
            self.mapi_folder_permission_audits
                .lock()
                .unwrap()
                .push(audit);
            Ok(())
        })
    }

    fn fetch_mapi_notification_cursor<'a>(
        &'a self,
        _account_id: Uuid,
    ) -> StoreFuture<'a, Option<i64>> {
        let cursor = *self.mapi_notification_cursor.lock().unwrap();
        Box::pin(async move { Ok(cursor) })
    }

    fn poll_mapi_notifications<'a>(
        &'a self,
        _account_id: Uuid,
        _after_cursor: i64,
    ) -> StoreFuture<'a, MapiNotificationPoll> {
        let poll = self
            .mapi_notification_polls
            .lock()
            .unwrap()
            .pop()
            .unwrap_or(MapiNotificationPoll {
                event_pending: false,
                cursor: None,
                events: Vec::new(),
            });
        Box::pin(async move { Ok(poll) })
    }

    fn fetch_address_book_entries<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<ExchangeAddressBookEntry>> {
        let principal_account = self.session.clone().filter(|account| {
            account.tenant_id == principal.tenant_id && account.account_id == principal.account_id
        });
        let mut accounts = self.directory_accounts.lock().unwrap().clone();
        if let Some(principal) = &principal_account {
            if !self.omit_principal_from_directory
                && !accounts
                    .iter()
                    .any(|account| account.account_id == principal.account_id)
            {
                accounts.push(principal.clone());
            }
            accounts.retain(|account| account.tenant_id == principal.tenant_id);
        } else {
            accounts.clear();
        }
        let mut entries = accounts
            .into_iter()
            .map(|account| ExchangeAddressBookEntry {
                id: account.account_id,
                display_name: account.display_name,
                email: account.email,
                entry_kind: ExchangeAddressBookEntryKind::Account,
                directory_kind: ExchangeAddressBookDirectoryKind::Person,
                member_emails: Vec::new(),
                details: ExchangeAddressBookEntryDetails::default(),
            })
            .collect::<Vec<_>>();
        let principal_account_id = principal_account
            .as_ref()
            .map(|account| account.account_id)
            .unwrap_or_default();
        let visible_collection_ids = self
            .contact_collections
            .lock()
            .unwrap()
            .iter()
            .filter(|collection| {
                collection.owner_account_id == principal_account_id || collection.rights.may_read
            })
            .map(|collection| collection.id.clone())
            .collect::<Vec<_>>();
        entries.extend(
            self.contacts
                .lock()
                .unwrap()
                .iter()
                .filter(|contact| {
                    contact.owner_account_id == principal_account_id
                        || visible_collection_ids.contains(&contact.collection_id)
                })
                .map(|contact| ExchangeAddressBookEntry {
                    id: contact.id,
                    display_name: contact.name.clone(),
                    email: contact.email.clone(),
                    entry_kind: ExchangeAddressBookEntryKind::Contact,
                    directory_kind: ExchangeAddressBookDirectoryKind::Person,
                    member_emails: Vec::new(),
                    details: ExchangeAddressBookEntryDetails {
                        given_name: contact.structured_name.given.clone(),
                        surname: contact.structured_name.family.clone(),
                        nickname: contact.structured_name.nickname.clone(),
                        primary_phone: contact.phone.clone(),
                        mobile_phone: fake_contact_phone_by_label(contact, &["mobile", "cell"]),
                        home_phone: fake_contact_phone_by_label(contact, &["home"]),
                        business2_phones: fake_contact_phone_values_by_label(
                            contact,
                            &["work2", "business2"],
                        ),
                        company_name: contact.organization_name.clone(),
                        title: contact.job_title.clone(),
                        department_name: contact.team.clone(),
                        postal_address: fake_contact_address_value(contact, &["full", "address"]),
                        street_address: fake_contact_address_value(
                            contact,
                            &["street", "streetAddress", "address"],
                        ),
                        locality: fake_contact_address_value(contact, &["city", "locality"]),
                        state_or_province: fake_contact_address_value(
                            contact,
                            &["state", "region", "stateOrProvince"],
                        ),
                        country: fake_contact_address_value(contact, &["country"]),
                        postal_code: fake_contact_address_value(
                            contact,
                            &["postcode", "postalCode", "zip"],
                        ),
                        phonetic_given_name: contact.structured_name.phonetic_given.clone(),
                        phonetic_surname: contact.structured_name.phonetic_family.clone(),
                    },
                }),
        );
        let group_alias_members = self.group_alias_members.lock().unwrap().clone();
        entries.extend(self.group_aliases.lock().unwrap().iter().map(
            |(id, display_name, email)| ExchangeAddressBookEntry {
                id: *id,
                display_name: display_name.clone(),
                email: email.clone(),
                entry_kind: ExchangeAddressBookEntryKind::DistributionList,
                directory_kind: ExchangeAddressBookDirectoryKind::Person,
                member_emails: group_alias_members.get(id).cloned().unwrap_or_default(),
                details: ExchangeAddressBookEntryDetails::default(),
            },
        ));
        let extra_entry_tenants = self
            .extra_address_book_entry_tenants
            .lock()
            .unwrap()
            .clone();
        let hidden_entry_ids = self.hidden_address_book_entry_ids.lock().unwrap().clone();
        entries.extend(
            self.extra_address_book_entries
                .lock()
                .unwrap()
                .iter()
                .filter(|entry| {
                    !hidden_entry_ids.contains(&entry.id)
                        && extra_entry_tenants
                            .get(&entry.id)
                            .copied()
                            .unwrap_or(principal.tenant_id)
                            == principal.tenant_id
                })
                .cloned(),
        );
        entries.sort_by(|left, right| {
            left.display_name
                .cmp(&right.display_name)
                .then_with(|| left.email.cmp(&right.email))
        });
        Box::pin(async move { Ok(entries) })
    }

    fn fetch_ews_im_list<'a>(
        &'a self,
        _principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, EwsImList> {
        let groups = self.ews_im_groups.lock().unwrap().clone();
        let members = self.ews_im_group_members.lock().unwrap().clone();
        Box::pin(async move { Ok(EwsImList { groups, members }) })
    }

    fn upsert_ews_im_group<'a>(
        &'a self,
        _principal: &'a AccountPrincipal,
        group_id: Option<Uuid>,
        display_name: &'a str,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, EwsImGroup> {
        let mut groups = self.ews_im_groups.lock().unwrap();
        let id = group_id
            .unwrap_or_else(|| Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap());
        let group = if let Some(existing) = groups.iter_mut().find(|group| group.id == id) {
            existing.display_name = display_name.to_string();
            existing.modseq += 1;
            existing.clone()
        } else {
            let group = EwsImGroup {
                id,
                display_name: display_name.to_string(),
                modseq: 1,
            };
            groups.push(group.clone());
            group
        };
        Box::pin(async move { Ok(group) })
    }

    fn remove_ews_im_group<'a>(
        &'a self,
        _principal: &'a AccountPrincipal,
        group_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, bool> {
        let mut groups = self.ews_im_groups.lock().unwrap();
        let before = groups.len();
        groups.retain(|group| group.id != group_id);
        let removed = groups.len() != before;
        if removed {
            self.ews_im_group_members
                .lock()
                .unwrap()
                .retain(|member| member.group_id != group_id);
        }
        Box::pin(async move { Ok(removed) })
    }

    fn add_ews_im_group_member<'a>(
        &'a self,
        _principal: &'a AccountPrincipal,
        group_id: Uuid,
        member: EwsImMemberInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, EwsImGroupMember> {
        if !self
            .ews_im_groups
            .lock()
            .unwrap()
            .iter()
            .any(|group| group.id == group_id)
        {
            return Box::pin(async move { Err(anyhow::anyhow!("IM group not found")) });
        }
        let mut members = self.ews_im_group_members.lock().unwrap();
        let existing = members.iter_mut().find(|existing| {
            existing.group_id == group_id
                && existing.member_kind == member.member_kind
                && existing.contact_id == member.contact_id
                && existing.account_id == member.account_id
                && existing
                    .external_address
                    .as_ref()
                    .map(|value| value.to_ascii_lowercase())
                    == member
                        .external_address
                        .as_ref()
                        .map(|value| value.to_ascii_lowercase())
        });
        let stored = if let Some(existing) = existing {
            existing.display_name = member.display_name;
            existing.clone()
        } else {
            let stored = EwsImGroupMember {
                id: Uuid::parse_str("34343434-3434-3434-3434-343434343434").unwrap(),
                group_id,
                member_kind: member.member_kind,
                contact_id: member.contact_id,
                account_id: member.account_id,
                external_address: member.external_address,
                display_name: member.display_name,
            };
            members.push(stored.clone());
            stored
        };
        Box::pin(async move { Ok(stored) })
    }

    fn remove_ews_im_group_member<'a>(
        &'a self,
        _principal: &'a AccountPrincipal,
        group_id: Option<Uuid>,
        member_kind: &'a str,
        member_value: &'a str,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, bool> {
        let value = member_value.to_ascii_lowercase();
        let mut members = self.ews_im_group_members.lock().unwrap();
        let before = members.len();
        members.retain(|member| {
            if group_id.map(|id| id != member.group_id).unwrap_or(false) {
                return true;
            }
            if member.member_kind != member_kind {
                return true;
            }
            let matches = match member_kind {
                "contact" => member.contact_id.map(|id| id.to_string()) == Some(value.clone()),
                "account" => member.account_id.map(|id| id.to_string()) == Some(value.clone()),
                _ => {
                    member
                        .external_address
                        .as_ref()
                        .map(|address| address.to_ascii_lowercase())
                        == Some(value.clone())
                }
            };
            !matches
        });
        let removed = members.len() != before;
        Box::pin(async move { Ok(removed) })
    }

    fn fetch_accessible_contact_collections<'a>(
        &'a self,
        _principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>> {
        let collections = self.contact_collections.lock().unwrap().clone();
        Box::pin(async move { Ok(collections) })
    }

    fn fetch_accessible_calendar_collections<'a>(
        &'a self,
        _principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>> {
        let collections = self.calendar_collections.lock().unwrap().clone();
        Box::pin(async move { Ok(collections) })
    }

    fn fetch_accessible_task_collections<'a>(
        &'a self,
        _principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>> {
        let collections = self.task_collections.lock().unwrap().clone();
        Box::pin(async move { Ok(collections) })
    }

    fn fetch_delegate_freebusy_messages<'a>(
        &'a self,
        _principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<DelegateFreeBusyMessageObject>> {
        let messages = self.delegate_freebusy_messages.lock().unwrap().clone();
        Box::pin(async move { Ok(messages) })
    }

    fn fetch_accessible_contacts_in_collection<'a>(
        &'a self,
        _principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<AccessibleContact>> {
        let contacts = self
            .contacts
            .lock()
            .unwrap()
            .iter()
            .filter(|contact| contact.collection_id == collection_id)
            .cloned()
            .collect();
        Box::pin(async move { Ok(contacts) })
    }

    fn fetch_contact_sync_versions<'a>(
        &'a self,
        _principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>> {
        let versions = self.contact_versions.lock().unwrap().clone();
        let contacts = self
            .contacts
            .lock()
            .unwrap()
            .iter()
            .filter(|contact| contact.collection_id == collection_id)
            .map(|contact| {
                (
                    contact.id,
                    versions
                        .get(&contact.id)
                        .copied()
                        .unwrap_or_default()
                        .to_string(),
                )
            })
            .collect();
        Box::pin(async move { Ok(contacts) })
    }

    fn fetch_accessible_events_in_collection<'a>(
        &'a self,
        _principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<AccessibleEvent>> {
        let events = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.collection_id == collection_id)
            .cloned()
            .collect();
        Box::pin(async move { Ok(events) })
    }

    fn fetch_event_sync_versions<'a>(
        &'a self,
        _principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>> {
        let versions = self.event_versions.lock().unwrap().clone();
        let events = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.collection_id == collection_id)
            .map(|event| {
                (
                    event.id,
                    versions
                        .get(&event.id)
                        .copied()
                        .unwrap_or_default()
                        .to_string(),
                )
            })
            .collect();
        Box::pin(async move { Ok(events) })
    }

    fn fetch_accessible_tasks_in_collection<'a>(
        &'a self,
        _principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<ClientTask>> {
        let tasks = self
            .tasks
            .lock()
            .unwrap()
            .iter()
            .filter(|task| {
                matches!(collection_id, "tasks" | "default")
                    || task.task_list_id.to_string() == collection_id
            })
            .cloned()
            .collect();
        Box::pin(async move { Ok(tasks) })
    }

    fn fetch_task_sync_versions<'a>(
        &'a self,
        _principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>> {
        let versions = self.task_versions.lock().unwrap().clone();
        let tasks = self
            .tasks
            .lock()
            .unwrap()
            .iter()
            .filter(|task| {
                matches!(collection_id, "tasks" | "default")
                    || task.task_list_id.to_string() == collection_id
            })
            .map(|task| {
                (
                    task.id,
                    versions
                        .get(&task.id)
                        .copied()
                        .map(|version| version.to_string())
                        .unwrap_or_else(|| task.updated_at.clone()),
                )
            })
            .collect();
        Box::pin(async move { Ok(tasks) })
    }

    fn fetch_accessible_contacts_by_ids<'a>(
        &'a self,
        _principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<AccessibleContact>> {
        let contacts = self
            .contacts
            .lock()
            .unwrap()
            .iter()
            .filter(|contact| ids.contains(&contact.id))
            .cloned()
            .collect();
        Box::pin(async move { Ok(contacts) })
    }

    fn create_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, AccessibleContact> {
        let account = Self::account();
        let contact = AccessibleContact {
            id: input.id.unwrap_or_else(|| {
                Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap()
            }),
            collection_id: collection_id.unwrap_or("default").to_string(),
            owner_account_id: principal_account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            rights: Self::rights(),
            name: input.name,
            role: input.role,
            email: input.email,
            phone: input.phone,
            team: input.team,
            notes: input.notes,
            ..Default::default()
        };
        self.contact_versions.lock().unwrap().insert(contact.id, 1);
        self.contacts.lock().unwrap().push(contact.clone());
        Box::pin(async move { Ok(contact) })
    }

    fn update_accessible_contact<'a>(
        &'a self,
        _principal_account_id: Uuid,
        contact_id: Uuid,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, AccessibleContact> {
        let mut contacts = self.contacts.lock().unwrap();
        let contact = contacts
            .iter_mut()
            .find(|contact| contact.id == contact_id)
            .unwrap();
        contact.name = input.name;
        contact.role = input.role;
        contact.email = input.email;
        contact.phone = input.phone;
        contact.team = input.team;
        contact.notes = input.notes;
        let mut versions = self.contact_versions.lock().unwrap();
        let version = versions
            .get(&contact_id)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        versions.insert(contact_id, version);
        let contact = contact.clone();
        Box::pin(async move { Ok(contact) })
    }

    fn delete_accessible_contact<'a>(
        &'a self,
        _principal_account_id: Uuid,
        contact_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        self.deleted_contacts.lock().unwrap().push(contact_id);
        self.contacts
            .lock()
            .unwrap()
            .retain(|contact| contact.id != contact_id);
        Box::pin(async move { Ok(()) })
    }

    fn fetch_accessible_events_by_ids<'a>(
        &'a self,
        _principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<AccessibleEvent>> {
        let events = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| ids.contains(&event.id))
            .cloned()
            .collect();
        Box::pin(async move { Ok(events) })
    }

    fn create_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, AccessibleEvent> {
        let account = Self::account();
        let event = AccessibleEvent {
            id: input.id.unwrap_or_else(|| {
                Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap()
            }),
            uid: input.uid,
            collection_id: collection_id.unwrap_or("default").to_string(),
            owner_account_id: principal_account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            rights: Self::rights(),
            date: input.date,
            time: input.time,
            time_zone: input.time_zone,
            duration_minutes: input.duration_minutes,
            all_day: input.all_day,
            status: input.status,
            sequence: input.sequence,
            recurrence_rule: input.recurrence_rule,
            recurrence_json: input.recurrence_json,
            recurrence_exceptions_json: input.recurrence_exceptions_json,
            title: input.title,
            location: input.location,
            organizer_json: input.organizer_json,
            attendees: input.attendees,
            attendees_json: input.attendees_json,
            notes: input.notes,
            body_html: input.body_html,
        };
        self.event_versions.lock().unwrap().insert(event.id, 1);
        self.events.lock().unwrap().push(event.clone());
        Box::pin(async move { Ok(event) })
    }

    fn update_accessible_event<'a>(
        &'a self,
        _principal_account_id: Uuid,
        event_id: Uuid,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, AccessibleEvent> {
        let mut events = self.events.lock().unwrap();
        let event = events
            .iter_mut()
            .find(|event| event.id == event_id)
            .unwrap();
        event.date = input.date;
        event.time = input.time;
        event.time_zone = input.time_zone;
        event.duration_minutes = input.duration_minutes;
        event.all_day = input.all_day;
        event.status = input.status;
        event.sequence = input.sequence;
        event.recurrence_rule = input.recurrence_rule;
        event.recurrence_json = input.recurrence_json;
        event.recurrence_exceptions_json = input.recurrence_exceptions_json;
        event.title = input.title;
        event.location = input.location;
        event.organizer_json = input.organizer_json;
        event.attendees = input.attendees;
        event.attendees_json = input.attendees_json;
        event.notes = input.notes;
        event.body_html = input.body_html;
        let mut versions = self.event_versions.lock().unwrap();
        let version = versions
            .get(&event_id)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        versions.insert(event_id, version);
        let event = event.clone();
        Box::pin(async move { Ok(event) })
    }

    fn update_accessible_event_reminder<'a>(
        &'a self,
        _principal_account_id: Uuid,
        event_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
    ) -> StoreFuture<'a, ()> {
        let mut reminders = self.reminders.lock().unwrap();
        reminders.retain(|reminder| {
            !(reminder.source_type == "calendar" && reminder.source_id == event_id)
        });
        if reminder_set.unwrap_or(false) {
            let event = self
                .events
                .lock()
                .unwrap()
                .iter()
                .find(|event| event.id == event_id)
                .cloned()
                .unwrap();
            reminders.push(ClientReminder {
                source_type: "calendar".to_string(),
                source_id: event_id,
                occurrence_start_at: None,
                title: event.title,
                due_at: Some(format!("{}T{}:00Z", event.date, event.time)),
                reminder_at: reminder_at.unwrap(),
                dismissed_at: reminder_dismissed_at,
                completed_at: None,
                status: "pending".to_string(),
            });
        }
        Box::pin(async move { Ok(()) })
    }

    fn delete_accessible_event<'a>(
        &'a self,
        _principal_account_id: Uuid,
        event_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        self.deleted_events.lock().unwrap().push(event_id);
        self.events
            .lock()
            .unwrap()
            .retain(|event| event.id != event_id);
        Box::pin(async move { Ok(()) })
    }

    fn fetch_accessible_tasks_by_ids<'a>(
        &'a self,
        _principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientTask>> {
        let tasks = self
            .tasks
            .lock()
            .unwrap()
            .iter()
            .filter(|task| ids.contains(&task.id))
            .cloned()
            .collect();
        Box::pin(async move { Ok(tasks) })
    }

    fn fetch_active_sieve_script<'a>(
        &'a self,
        _account_id: Uuid,
    ) -> StoreFuture<'a, Option<SieveScriptDocument>> {
        let content = self.active_sieve_script.lock().unwrap().clone();
        Box::pin(async move {
            Ok(content.map(|content| SieveScriptDocument {
                name: "jmap-vacation".to_string(),
                content,
                is_active: true,
                updated_at: "2026-05-05T08:00:00Z".to_string(),
            }))
        })
    }

    fn list_mailbox_rules<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<MailboxRule>> {
        let rules = self.mailbox_rules.lock().unwrap().clone();
        Box::pin(async move { Ok(rules) })
    }

    fn put_sieve_script<'a>(
        &'a self,
        _account_id: Uuid,
        name: &'a str,
        content: &'a str,
        activate: bool,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptDocument> {
        if activate {
            *self.active_sieve_script.lock().unwrap() = Some(content.to_string());
        }
        let (condition_summary, action_summary) =
            if let Some((condition, action)) = content.split_once('{') {
                (
                    condition.trim().to_string(),
                    action.trim_end_matches('}').trim().to_string(),
                )
            } else {
                (String::new(), content.to_string())
            };
        let mut rules = self.mailbox_rules.lock().unwrap();
        if let Some(rule) = rules.iter_mut().find(|rule| rule.name == name) {
            rule.is_active = activate;
            rule.condition_summary = condition_summary;
            rule.action_summary = action_summary;
            rule.size_octets = content.len() as u64;
        } else {
            rules.push(MailboxRule {
                id: Uuid::new_v4(),
                name: name.to_string(),
                is_active: activate,
                source_kind: "sieve_script".to_string(),
                condition_summary,
                action_summary,
                supported_outlook_projection: true,
                unsupported_exchange_features: Vec::new(),
                size_octets: content.len() as u64,
                updated_at: "2026-05-05T08:00:00Z".to_string(),
            });
        }
        let script = SieveScriptDocument {
            name: name.to_string(),
            content: content.to_string(),
            is_active: activate,
            updated_at: "2026-05-05T08:00:00Z".to_string(),
        };
        Box::pin(async move { Ok(script) })
    }

    fn delete_sieve_script<'a>(
        &'a self,
        _account_id: Uuid,
        name: &'a str,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        self.mailbox_rules
            .lock()
            .unwrap()
            .retain(|rule| rule.name != name);
        Box::pin(async move { Ok(()) })
    }

    fn set_active_sieve_script<'a>(
        &'a self,
        _account_id: Uuid,
        name: Option<&'a str>,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, Option<String>> {
        if name.is_none() {
            *self.active_sieve_script.lock().unwrap() = None;
        }
        let active_name = name.map(str::to_string);
        Box::pin(async move { Ok(active_name) })
    }

    fn create_accessible_task<'a>(
        &'a self,
        principal_account_id: Uuid,
        input: UpsertClientTaskInput,
    ) -> StoreFuture<'a, ClientTask> {
        let account = Self::account();
        let task = ClientTask {
            id: input.id.unwrap_or_else(|| {
                Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap()
            }),
            owner_account_id: principal_account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            is_owned: true,
            rights: Self::rights(),
            task_list_id: input.task_list_id.unwrap_or_else(|| {
                Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap()
            }),
            task_list_sort_order: 0,
            title: input.title,
            description: input.description,
            status: input.status,
            due_at: input.due_at,
            completed_at: input.completed_at,
            recurrence_rule: input.recurrence_rule,
            sort_order: input.sort_order,
            updated_at: "2026-05-05T08:00:00Z".to_string(),
        };
        self.task_versions.lock().unwrap().insert(task.id, 1);
        self.tasks.lock().unwrap().push(task.clone());
        Box::pin(async move { Ok(task) })
    }

    fn update_accessible_task<'a>(
        &'a self,
        _principal_account_id: Uuid,
        task_id: Uuid,
        input: UpsertClientTaskInput,
    ) -> StoreFuture<'a, ClientTask> {
        let mut tasks = self.tasks.lock().unwrap();
        let task = tasks.iter_mut().find(|task| task.id == task_id).unwrap();
        task.task_list_id = input.task_list_id.unwrap_or(task.task_list_id);
        task.title = input.title;
        task.description = input.description;
        task.status = input.status;
        task.due_at = input.due_at;
        task.completed_at = if task.status == "completed" {
            input
                .completed_at
                .or_else(|| Some("2026-05-05T10:00:00Z".to_string()))
        } else {
            None
        };
        task.sort_order = input.sort_order;
        let mut versions = self.task_versions.lock().unwrap();
        let version = versions
            .get(&task_id)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        versions.insert(task_id, version);
        let task = task.clone();
        Box::pin(async move { Ok(task) })
    }

    fn update_accessible_task_reminder<'a>(
        &'a self,
        _principal_account_id: Uuid,
        task_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
        _reminder_reset: Option<bool>,
    ) -> StoreFuture<'a, ()> {
        let mut reminders = self.reminders.lock().unwrap();
        reminders
            .retain(|reminder| !(reminder.source_type == "task" && reminder.source_id == task_id));
        if reminder_set.unwrap_or(false) {
            let task = self
                .tasks
                .lock()
                .unwrap()
                .iter()
                .find(|task| task.id == task_id)
                .cloned()
                .unwrap();
            reminders.push(ClientReminder {
                source_type: "task".to_string(),
                source_id: task_id,
                occurrence_start_at: None,
                title: task.title,
                due_at: task.due_at,
                reminder_at: reminder_at.unwrap(),
                dismissed_at: reminder_dismissed_at,
                completed_at: task.completed_at,
                status: "pending".to_string(),
            });
        }
        Box::pin(async move { Ok(()) })
    }

    fn delete_accessible_task<'a>(
        &'a self,
        _principal_account_id: Uuid,
        task_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        self.deleted_tasks.lock().unwrap().push(task_id);
        self.tasks.lock().unwrap().retain(|task| task.id != task_id);
        Box::pin(async move { Ok(()) })
    }

    fn fetch_mapi_notes<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<ClientNote>> {
        let notes = self.notes.lock().unwrap().clone();
        Box::pin(async move { Ok(notes) })
    }

    fn fetch_mapi_notes_by_ids<'a>(
        &'a self,
        _account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientNote>> {
        let notes = self
            .notes
            .lock()
            .unwrap()
            .iter()
            .filter(|note| ids.contains(&note.id))
            .cloned()
            .collect();
        Box::pin(async move { Ok(notes) })
    }

    fn fetch_mapi_journal_entries<'a>(
        &'a self,
        _account_id: Uuid,
    ) -> StoreFuture<'a, Vec<JournalEntry>> {
        let entries = self.journal_entries.lock().unwrap().clone();
        Box::pin(async move { Ok(entries) })
    }

    fn fetch_mapi_journal_entries_by_ids<'a>(
        &'a self,
        _account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JournalEntry>> {
        let entries = self
            .journal_entries
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| ids.contains(&entry.id))
            .cloned()
            .collect();
        Box::pin(async move { Ok(entries) })
    }

    fn upsert_mapi_note<'a>(&'a self, input: UpsertClientNoteInput) -> StoreFuture<'a, ClientNote> {
        let note = ClientNote {
            id: input.id.unwrap_or_else(|| {
                Uuid::parse_str("f1f1f1f1-f1f1-f1f1-f1f1-f1f1f1f1f1f1").unwrap()
            }),
            title: input.title,
            body_text: input.body_text,
            color: input.color,
            categories_json: input.categories_json,
            created_at: "2026-05-05T08:00:00Z".to_string(),
            updated_at: "2026-05-05T08:00:00Z".to_string(),
        };
        let mut notes = self.notes.lock().unwrap();
        notes.retain(|existing| existing.id != note.id);
        notes.push(note.clone());
        Box::pin(async move { Ok(note) })
    }

    fn upsert_mapi_journal_entry<'a>(
        &'a self,
        input: UpsertJournalEntryInput,
    ) -> StoreFuture<'a, JournalEntry> {
        let entry = JournalEntry {
            id: input.id.unwrap_or_else(|| {
                Uuid::parse_str("f2f2f2f2-f2f2-f2f2-f2f2-f2f2f2f2f2f2").unwrap()
            }),
            subject: input.subject,
            body_text: input.body_text,
            entry_type: input.entry_type,
            message_class: input.message_class,
            starts_at: input.starts_at,
            ends_at: input.ends_at,
            occurred_at: input.occurred_at,
            companies_json: input.companies_json,
            contacts_json: input.contacts_json,
            created_at: "2026-05-05T08:00:00Z".to_string(),
            updated_at: "2026-05-05T08:00:00Z".to_string(),
        };
        let mut entries = self.journal_entries.lock().unwrap();
        entries.retain(|existing| existing.id != entry.id);
        entries.push(entry.clone());
        Box::pin(async move { Ok(entry) })
    }

    fn fetch_jmap_mailboxes<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        let mailboxes = self.mailboxes.lock().unwrap().clone();
        let load_started = self.mapi_mail_store_load_started.clone();
        let load_continue = self.mapi_mail_store_load_continue.clone();
        Box::pin(async move {
            if let Some(load_started) = load_started {
                load_started.notify_one();
            }
            if let Some(load_continue) = load_continue {
                load_continue.notified().await;
            }
            Ok(mailboxes)
        })
    }

    fn ensure_jmap_system_mailboxes<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<JmapMailbox>> {
        self.fetch_jmap_mailboxes(account_id)
    }

    fn fetch_search_folders<'a>(
        &'a self,
        _account_id: Uuid,
    ) -> StoreFuture<'a, Vec<SearchFolderDefinition>> {
        let search_folders = self.search_folders.lock().unwrap().clone();
        Box::pin(async move { Ok(search_folders) })
    }

    fn upsert_search_folder<'a>(
        &'a self,
        input: UpsertSearchFolderInput,
    ) -> StoreFuture<'a, SearchFolderDefinition> {
        let search_folders = self.search_folders.clone();
        Box::pin(async move {
            let mut search_folders = search_folders.lock().unwrap();
            let id = input.id.unwrap_or_else(Uuid::new_v4);
            let definition = SearchFolderDefinition {
                id,
                account_id: input.account_id,
                role: "custom".to_string(),
                display_name: input.display_name,
                definition_kind: "user_saved".to_string(),
                result_object_kind: input.result_object_kind,
                scope_json: input.scope_json,
                restriction_json: input.restriction_json,
                excluded_folder_roles: input.excluded_folder_roles,
                is_builtin: false,
            };
            if let Some(existing) = search_folders.iter_mut().find(|folder| folder.id == id) {
                if existing.is_builtin {
                    return Err(anyhow::anyhow!("builtin search folders cannot be updated"));
                }
                *existing = definition.clone();
            } else {
                search_folders.push(definition.clone());
            }
            Ok(definition)
        })
    }

    fn delete_search_folder<'a>(
        &'a self,
        _account_id: Uuid,
        search_folder_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        let search_folders = self.search_folders.clone();
        let deleted_search_folders = self.deleted_search_folders.clone();
        Box::pin(async move {
            let mut search_folders = search_folders.lock().unwrap();
            let Some(index) = search_folders
                .iter()
                .position(|folder| folder.id == search_folder_id)
            else {
                return Err(anyhow::anyhow!("search folder not found"));
            };
            if search_folders[index].is_builtin {
                return Err(anyhow::anyhow!("builtin search folders cannot be deleted"));
            }
            search_folders.remove(index);
            deleted_search_folders
                .lock()
                .unwrap()
                .push(search_folder_id);
            Ok(())
        })
    }

    fn fetch_conversation_actions<'a>(
        &'a self,
        _account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ConversationAction>> {
        let conversation_actions = self.conversation_actions.lock().unwrap().clone();
        Box::pin(async move { Ok(conversation_actions) })
    }

    fn fetch_mapi_navigation_shortcuts<'a>(
        &'a self,
        _account_id: Uuid,
    ) -> StoreFuture<'a, Vec<crate::store::MapiNavigationShortcutRecord>> {
        let shortcuts = self.navigation_shortcuts.lock().unwrap().clone();
        Box::pin(async move { Ok(shortcuts) })
    }

    fn upsert_mapi_navigation_shortcut<'a>(
        &'a self,
        input: crate::store::UpsertMapiNavigationShortcutInput,
    ) -> StoreFuture<'a, crate::store::MapiNavigationShortcutRecord> {
        let shortcuts = self.navigation_shortcuts.clone();
        Box::pin(async move {
            let mut shortcuts = shortcuts.lock().unwrap();
            let id = input.id.unwrap_or_else(Uuid::new_v4);
            let record = crate::store::MapiNavigationShortcutRecord {
                id,
                account_id: input.account_id,
                subject: input.subject,
                target_folder_id: input.target_folder_id,
                shortcut_type: input.shortcut_type,
                flags: input.flags,
                save_stamp: input.save_stamp,
                section: input.section,
                ordinal: input.ordinal,
                group_header_id: input.group_header_id,
                group_name: input.group_name,
            };
            if let Some(existing) = shortcuts.iter_mut().find(|shortcut| shortcut.id == id) {
                *existing = record.clone();
            } else {
                shortcuts.push(record.clone());
            }
            Ok(record)
        })
    }

    fn delete_mapi_navigation_shortcut<'a>(
        &'a self,
        _account_id: Uuid,
        shortcut_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        self.navigation_shortcuts
            .lock()
            .unwrap()
            .retain(|shortcut| shortcut.id != shortcut_id);
        Box::pin(async move { Ok(()) })
    }

    fn fetch_mapi_associated_configs<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<crate::store::MapiAssociatedConfigRecord>> {
        let configs = self
            .associated_configs
            .lock()
            .unwrap()
            .iter()
            .filter(|config| config.account_id == account_id)
            .cloned()
            .collect();
        Box::pin(async move { Ok(configs) })
    }

    fn upsert_mapi_associated_config<'a>(
        &'a self,
        input: crate::store::UpsertMapiAssociatedConfigInput,
    ) -> StoreFuture<'a, crate::store::MapiAssociatedConfigRecord> {
        let configs = self.associated_configs.clone();
        let changes = self.mapi_sync_changes.clone();
        Box::pin(async move {
            let mut configs = configs.lock().unwrap();
            let id = input.id.unwrap_or_else(Uuid::new_v4);
            if configs
                .iter()
                .any(|config| config.id == id && config.account_id != input.account_id)
            {
                return Err(anyhow::anyhow!("MAPI associated config message not found"));
            }
            let is_update = configs
                .iter()
                .any(|config| config.id == id && config.account_id == input.account_id);
            let record = crate::store::MapiAssociatedConfigRecord {
                id,
                account_id: input.account_id,
                folder_id: input.folder_id,
                message_class: input.message_class,
                subject: input.subject,
                properties_json: input.properties_json,
            };
            if let Some(existing) = configs
                .iter_mut()
                .find(|config| config.id == id && config.account_id == input.account_id)
            {
                *existing = record.clone();
            } else {
                configs.push(record.clone());
            }
            let mut changes = changes.lock().unwrap();
            changes.current_change_sequence = changes.current_change_sequence.saturating_add(1);
            changes.current_modseq = changes.current_modseq.saturating_add(1);
            if is_update {
                changes
                    .changed_associated_config_ids
                    .retain(|change| change.config_id != id);
            }
            changes
                .changed_associated_config_ids
                .push(crate::store::MapiAssociatedConfigChange {
                    folder_id: record.folder_id,
                    config_id: record.id,
                });
            Ok(record)
        })
    }

    fn delete_mapi_associated_config<'a>(
        &'a self,
        account_id: Uuid,
        config_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        let mut configs = self.associated_configs.lock().unwrap();
        if let Some(folder_id) = configs
            .iter()
            .find(|config| config.id == config_id && config.account_id == account_id)
            .map(|config| config.folder_id)
        {
            let mut changes = self.mapi_sync_changes.lock().unwrap();
            changes.current_change_sequence = changes.current_change_sequence.saturating_add(1);
            changes.current_modseq = changes.current_modseq.saturating_add(1);
            changes
                .changed_associated_config_ids
                .retain(|change| change.config_id != config_id);
            changes
                .deleted_associated_config_ids
                .push(crate::store::MapiAssociatedConfigChange {
                    folder_id,
                    config_id,
                });
        }
        configs.retain(|config| !(config.id == config_id && config.account_id == account_id));
        Box::pin(async move { Ok(()) })
    }

    fn upsert_conversation_action<'a>(
        &'a self,
        input: UpsertConversationActionInput,
    ) -> StoreFuture<'a, ConversationAction> {
        Box::pin(async move {
            let action = ConversationAction {
                id: input.conversation_id,
                conversation_id: input.conversation_id,
                subject: input.subject,
                categories_json: input.categories_json,
                move_folder_entry_id: input.move_folder_entry_id,
                move_store_entry_id: input.move_store_entry_id,
                move_target_mailbox_id: input.move_target_mailbox_id,
                max_delivery_time: input.max_delivery_time,
                last_applied_time: input.last_applied_time,
                version: input
                    .version
                    .unwrap_or(lpe_storage::CONVERSATION_ACTION_VERSION),
                processed: input.processed.unwrap_or_default(),
                created_at: "2026-05-22T00:00:00Z".to_string(),
                updated_at: "2026-05-22T00:00:00Z".to_string(),
            };
            let mut actions = self.conversation_actions.lock().unwrap();
            if let Some(existing) = actions
                .iter_mut()
                .find(|existing| existing.conversation_id == action.conversation_id)
            {
                *existing = action.clone();
            } else {
                actions.push(action.clone());
            }
            Ok(action)
        })
    }

    fn delete_conversation_action<'a>(
        &'a self,
        _account_id: Uuid,
        conversation_action_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        self.conversation_actions
            .lock()
            .unwrap()
            .retain(|action| action.id != conversation_action_id);
        Box::pin(async move { Ok(()) })
    }

    fn query_client_reminders<'a>(
        &'a self,
        _account_id: Uuid,
        _query: ReminderQuery,
    ) -> StoreFuture<'a, Vec<ClientReminder>> {
        let reminders = self.reminders.lock().unwrap().clone();
        Box::pin(async move { Ok(reminders) })
    }

    fn dismiss_reminder_occurrence<'a>(
        &'a self,
        _account_id: Uuid,
        source_type: &'a str,
        source_id: Uuid,
        occurrence_start_at: Option<&'a str>,
        dismissed_at: &'a str,
    ) -> StoreFuture<'a, ()> {
        let occurrence_start_at = occurrence_start_at.map(str::to_string);
        let dismissed_at = dismissed_at.to_string();
        let source_type = source_type.to_string();
        let mut reminders = self.reminders.lock().unwrap();
        for reminder in reminders.iter_mut().filter(|reminder| {
            reminder.source_type == source_type
                && reminder.source_id == source_id
                && reminder.occurrence_start_at == occurrence_start_at
        }) {
            reminder.dismissed_at = Some(dismissed_at.clone());
            reminder.status = "dismissed".to_string();
        }
        Box::pin(async move { Ok(()) })
    }

    fn create_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxCreateInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        self.created_mailboxes.lock().unwrap().push(input.clone());
        let mailbox_id = if self.mailboxes.lock().unwrap().is_empty() {
            Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap()
        } else {
            Uuid::from_u128(
                0x44444444_4444_4444_4444_444444444400
                    + self.mailboxes.lock().unwrap().len() as u128
                    + 1,
            )
        };
        let mailbox = JmapMailbox {
            id: mailbox_id,
            parent_id: input.parent_id,
            role: "custom".to_string(),
            name: input.name,
            sort_order: input.sort_order.unwrap_or(40),
            modseq: 40,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: input.is_subscribed,
        };
        self.mailboxes.lock().unwrap().push(mailbox.clone());
        self.mapi_sync_changes
            .lock()
            .unwrap()
            .changed_mailbox_ids
            .push(mailbox.id);
        Box::pin(async move { Ok(mailbox) })
    }

    fn update_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxUpdateInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        self.updated_mailboxes.lock().unwrap().push(input.clone());
        let mut mailboxes = self.mailboxes.lock().unwrap();
        let Some(mailbox) = mailboxes
            .iter_mut()
            .find(|mailbox| mailbox.id == input.mailbox_id)
        else {
            return Box::pin(async move { Err(anyhow::anyhow!("mailbox not found")) });
        };
        if let Some(name) = input.name.clone() {
            mailbox.name = name;
        }
        if let Some(parent_id) = input.parent_id {
            mailbox.parent_id = parent_id;
        }
        mailbox.modseq += 1;
        let mailbox = mailbox.clone();
        self.mapi_sync_changes
            .lock()
            .unwrap()
            .changed_mailbox_ids
            .push(mailbox.id);
        Box::pin(async move { Ok(mailbox) })
    }

    fn destroy_jmap_mailbox<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        self.destroyed_mailboxes.lock().unwrap().push(mailbox_id);
        self.mailboxes
            .lock()
            .unwrap()
            .retain(|mailbox| mailbox.id != mailbox_id);
        self.mapi_sync_changes
            .lock()
            .unwrap()
            .changed_mailbox_ids
            .push(mailbox_id);
        Box::pin(async move { Ok(()) })
    }

    fn query_jmap_email_ids<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&'a str>,
        _position: u64,
        _limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery> {
        self.queried_jmap_email_ids.fetch_add(1, Ordering::SeqCst);
        if self.fail_query_jmap_email_ids {
            return Box::pin(async move { Err(anyhow::anyhow!("forced email query failure")) });
        }
        let ids = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| mailbox_id.map_or(true, |mailbox_id| email.mailbox_id == mailbox_id))
            .filter(|email| {
                search_text.map_or(true, |search_text| jmap_search_matches(email, search_text))
            })
            .map(|email| email.id)
            .collect::<Vec<_>>();
        Box::pin(async move {
            Ok(JmapEmailQuery {
                total: ids.len() as u64,
                ids,
            })
        })
    }

    fn query_mapi_content_table_ids<'a>(
        &'a self,
        _account_id: Uuid,
        query: MapiContentTableQuery,
    ) -> StoreFuture<'a, MapiContentTableQueryResult> {
        let mut emails = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| email.mailbox_id == query.mailbox_id)
            .cloned()
            .collect::<Vec<_>>();
        if !query.sort_orders.is_empty() {
            emails.sort_by(|left, right| {
                for sort in &query.sort_orders {
                    let ordering = match sort.field {
                        MapiContentTableSortField::ReceivedAt => {
                            left.received_at.cmp(&right.received_at)
                        }
                        MapiContentTableSortField::Subject => left
                            .subject
                            .to_ascii_lowercase()
                            .cmp(&right.subject.to_ascii_lowercase()),
                        MapiContentTableSortField::SenderName => left
                            .from_display
                            .as_deref()
                            .unwrap_or(&left.from_address)
                            .to_ascii_lowercase()
                            .cmp(
                                &right
                                    .from_display
                                    .as_deref()
                                    .unwrap_or(&right.from_address)
                                    .to_ascii_lowercase(),
                            ),
                        MapiContentTableSortField::SenderEmail => left
                            .from_address
                            .to_ascii_lowercase()
                            .cmp(&right.from_address.to_ascii_lowercase()),
                        MapiContentTableSortField::DisplayTo => display_to_for_test(left)
                            .to_ascii_lowercase()
                            .cmp(&display_to_for_test(right).to_ascii_lowercase()),
                        MapiContentTableSortField::MessageSize => {
                            left.size_octets.cmp(&right.size_octets)
                        }
                        MapiContentTableSortField::HasAttachments => {
                            left.has_attachments.cmp(&right.has_attachments)
                        }
                        MapiContentTableSortField::MessageFlags => {
                            test_message_flags(left).cmp(&test_message_flags(right))
                        }
                    };
                    let ordering = if sort.descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                    if ordering != std::cmp::Ordering::Equal {
                        return ordering;
                    }
                }
                right.id.cmp(&left.id)
            });
        }
        let total = emails.len() as u64;
        let ids = emails
            .into_iter()
            .skip(query.position as usize)
            .take(query.limit as usize)
            .map(|email| email.id)
            .collect();
        Box::pin(async move { Ok(MapiContentTableQueryResult { ids, total }) })
    }

    fn fetch_all_jmap_email_ids<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<Uuid>> {
        let ids = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .map(|email| email.id)
            .collect();
        Box::pin(async move { Ok(ids) })
    }

    fn list_recoverable_items<'a>(
        &'a self,
        _account_id: Uuid,
        recoverable_folder: Option<&'a str>,
    ) -> StoreFuture<'a, Vec<RecoverableItem>> {
        let items = self
            .recoverable_items
            .lock()
            .unwrap()
            .iter()
            .filter(|item| item.status == "active")
            .filter(|item| {
                recoverable_folder
                    .map(|folder| item.recoverable_folder == folder)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        Box::pin(async move { Ok(items) })
    }

    fn restore_recoverable_item<'a>(
        &'a self,
        _account_id: Uuid,
        recoverable_item_id: Uuid,
        target_mailbox_id: Option<Uuid>,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        self.restored_recoverable_items
            .lock()
            .unwrap()
            .push((recoverable_item_id, target_mailbox_id));
        let item = self
            .recoverable_items
            .lock()
            .unwrap()
            .iter_mut()
            .find(|item| item.id == recoverable_item_id)
            .map(|item| {
                item.status = "restored".to_string();
                item.clone()
            });
        let target = target_mailbox_id.and_then(|target_id| {
            self.mailboxes
                .lock()
                .unwrap()
                .iter()
                .find(|mailbox| mailbox.id == target_id)
                .cloned()
        });
        Box::pin(async move {
            let item = item.ok_or_else(|| anyhow::anyhow!("recoverable item not found"))?;
            let mailbox_id = target
                .as_ref()
                .map(|mailbox| mailbox.id)
                .or(target_mailbox_id)
                .unwrap_or(item.source_mailbox_id);
            let mailbox_role = target
                .as_ref()
                .map(|mailbox| mailbox.role.as_str())
                .unwrap_or("restored");
            Ok(FakeStore::email(
                &item.message_id.to_string(),
                &mailbox_id.to_string(),
                mailbox_role,
                &item.subject,
            ))
        })
    }

    fn purge_recoverable_item<'a>(
        &'a self,
        _account_id: Uuid,
        recoverable_item_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        if self
            .failed_purge_recoverable_item_ids
            .lock()
            .unwrap()
            .contains(&recoverable_item_id)
        {
            return Box::pin(
                async move { Err(anyhow::anyhow!("forced recoverable purge failure")) },
            );
        }
        self.purged_recoverable_items
            .lock()
            .unwrap()
            .push(recoverable_item_id);
        self.recoverable_items
            .lock()
            .unwrap()
            .retain(|item| item.id != recoverable_item_id);
        Box::pin(async move { Ok(()) })
    }

    fn fetch_jmap_emails<'a>(
        &'a self,
        _account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>> {
        let emails = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| ids.contains(&email.id))
            .cloned()
            .map(|mut email| {
                email.bcc.clear();
                email
            })
            .collect();
        Box::pin(async move { Ok(emails) })
    }

    fn fetch_jmap_emails_with_protected_bcc<'a>(
        &'a self,
        _account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>> {
        let emails = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| ids.contains(&email.id))
            .cloned()
            .collect();
        Box::pin(async move { Ok(emails) })
    }

    fn fetch_message_attachments<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncAttachment>> {
        let attachments = self
            .attachments
            .lock()
            .unwrap()
            .get(&message_id)
            .cloned()
            .unwrap_or_default();
        Box::pin(async move { Ok(attachments) })
    }

    fn fetch_calendar_attachments_for_events<'a>(
        &'a self,
        _account_id: Uuid,
        event_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<(Uuid, Vec<CalendarEventAttachment>)>> {
        let attachments = self.calendar_attachments.lock().unwrap();
        let result = event_ids
            .iter()
            .map(|event_id| {
                (
                    *event_id,
                    attachments.get(event_id).cloned().unwrap_or_default(),
                )
            })
            .collect();
        Box::pin(async move { Ok(result) })
    }

    fn fetch_attachment_content<'a>(
        &'a self,
        _account_id: Uuid,
        file_reference: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncAttachmentContent>> {
        let content = self
            .attachment_contents
            .lock()
            .unwrap()
            .get(file_reference)
            .cloned();
        Box::pin(async move { Ok(content) })
    }

    fn add_message_attachment<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        attachment: AttachmentUploadInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, Option<(JmapEmail, ActiveSyncAttachment)>> {
        let mut emails = self.emails.lock().unwrap();
        let Some(email) = emails.iter_mut().find(|email| email.id == message_id) else {
            return Box::pin(async move { Ok(None) });
        };
        email.has_attachments = true;
        let email = email.clone();
        drop(emails);

        self.created_attachments
            .lock()
            .unwrap()
            .push(attachment.clone());
        let attachment_id = Uuid::parse_str("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd").unwrap();
        let file_reference = format!("attachment:{message_id}:{attachment_id}");
        let stored_attachment = ActiveSyncAttachment {
            id: attachment_id,
            message_id,
            file_name: attachment.file_name.clone(),
            media_type: attachment.media_type.clone(),
            disposition: attachment.disposition.clone(),
            content_id: attachment.content_id.clone(),
            size_octets: attachment.blob_bytes.len() as u64,
            file_reference: file_reference.clone(),
        };
        self.attachments
            .lock()
            .unwrap()
            .entry(message_id)
            .or_default()
            .push(stored_attachment.clone());
        self.attachment_contents.lock().unwrap().insert(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference,
                file_name: attachment.file_name,
                media_type: attachment.media_type,
                blob_bytes: attachment.blob_bytes,
            },
        );

        Box::pin(async move { Ok(Some((email, stored_attachment))) })
    }

    fn add_calendar_event_attachment<'a>(
        &'a self,
        _account_id: Uuid,
        event_id: Uuid,
        attachment: AttachmentUploadInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, Option<CalendarEventAttachment>> {
        if !self
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.id == event_id)
        {
            return Box::pin(async move { Ok(None) });
        }
        let attachment_id = Uuid::parse_str("cececece-cece-cece-cece-cececececece").unwrap();
        let stored = CalendarEventAttachment {
            id: attachment_id,
            event_id,
            file_name: attachment.file_name,
            media_type: attachment.media_type,
            size_octets: attachment.blob_bytes.len() as u64,
            file_reference: lpe_storage::calendar_attachment_file_reference(
                event_id,
                attachment_id,
            ),
        };
        self.calendar_attachments
            .lock()
            .unwrap()
            .entry(event_id)
            .or_default()
            .push(stored.clone());
        Box::pin(async move { Ok(Some(stored)) })
    }

    fn delete_message_attachment<'a>(
        &'a self,
        _account_id: Uuid,
        file_reference: &'a str,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, Option<JmapEmail>> {
        let Some((message_id, attachment_id)) = parse_attachment_reference(file_reference) else {
            return Box::pin(async move { Ok(None) });
        };
        let mut attachments = self.attachments.lock().unwrap();
        let Some(message_attachments) = attachments.get_mut(&message_id) else {
            return Box::pin(async move { Ok(None) });
        };
        let before_len = message_attachments.len();
        message_attachments.retain(|attachment| attachment.id != attachment_id);
        if message_attachments.len() == before_len {
            return Box::pin(async move { Ok(None) });
        }
        let has_attachments = !message_attachments.is_empty();
        drop(attachments);

        let mut emails = self.emails.lock().unwrap();
        let Some(email) = emails.iter_mut().find(|email| email.id == message_id) else {
            return Box::pin(async move { Ok(None) });
        };
        email.has_attachments = has_attachments;
        let email = email.clone();
        Box::pin(async move { Ok(Some(email)) })
    }

    fn delete_calendar_event_attachment<'a>(
        &'a self,
        _account_id: Uuid,
        file_reference: &'a str,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, Option<Uuid>> {
        let Some((event_id, attachment_id)) =
            lpe_storage::parse_calendar_attachment_file_reference(file_reference)
        else {
            return Box::pin(async move { Ok(None) });
        };
        let mut attachments = self.calendar_attachments.lock().unwrap();
        let Some(event_attachments) = attachments.get_mut(&event_id) else {
            return Box::pin(async move { Ok(None) });
        };
        let before_len = event_attachments.len();
        event_attachments.retain(|attachment| attachment.id != attachment_id);
        if event_attachments.len() == before_len {
            return Box::pin(async move { Ok(None) });
        }
        Box::pin(async move { Ok(Some(event_id)) })
    }

    fn import_jmap_email<'a>(
        &'a self,
        input: JmapImportedEmailInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        self.imported_emails.lock().unwrap().push(input.clone());
        let mailbox = self
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|mailbox| mailbox.id == input.mailbox_id)
            .cloned();
        let mut email = FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            &input.mailbox_id.to_string(),
            mailbox
                .as_ref()
                .map(|mailbox| mailbox.role.as_str())
                .unwrap_or("custom"),
            &input.subject,
        );
        if let Some(mailbox) = mailbox {
            email.mailbox_name = mailbox.name;
        }
        email.from_address = input.from_address;
        email.from_display = input.from_display;
        email.sender_address = input.sender_address;
        email.sender_display = input.sender_display;
        email.submitted_by_account_id = input.submitted_by_account_id;
        email.to = FakeStore::email_addresses(&input.to);
        email.cc = FakeStore::email_addresses(&input.cc);
        email.bcc = FakeStore::email_addresses(&input.bcc);
        email.preview = input.body_text.clone();
        email.body_text = input.body_text;
        email.body_html_sanitized = input.body_html_sanitized;
        email.internet_message_id = input.internet_message_id;
        email.mime_blob_ref = Some(input.mime_blob_ref);
        email.size_octets = input.size_octets;
        email.received_at = input
            .received_at
            .unwrap_or_else(|| "2026-05-07T12:00:00Z".to_string());
        if let Some(thread_id) = input.thread_id {
            email.thread_id = thread_id;
        }
        email.has_attachments = !input.attachments.is_empty();
        self.emails.lock().unwrap().push(email.clone());
        Box::pin(async move { Ok(email) })
    }

    fn move_jmap_email<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        self.moved_emails
            .lock()
            .unwrap()
            .push((message_id, target_mailbox_id));
        let target = self
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|mailbox| mailbox.id == target_mailbox_id)
            .cloned();
        let mut emails = self.emails.lock().unwrap();
        let email = emails
            .iter_mut()
            .find(|email| email.id == message_id)
            .unwrap();
        if let Some(target) = target {
            email.mailbox_id = target.id;
            email.mailbox_role = target.role;
            email.mailbox_name = target.name;
        } else {
            email.mailbox_id = target_mailbox_id;
        }
        let moved = email.clone();
        Box::pin(async move { Ok(moved) })
    }

    fn move_jmap_email_from_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        _source_mailbox_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        self.move_jmap_email(account_id, message_id, target_mailbox_id, audit)
    }

    fn copy_jmap_email<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        self.copied_emails
            .lock()
            .unwrap()
            .push((message_id, target_mailbox_id));
        let target = self
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|mailbox| mailbox.id == target_mailbox_id)
            .cloned();
        let mut copied = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .find(|email| email.id == message_id)
            .cloned()
            .unwrap();
        copied.id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
        if let Some(target) = target {
            copied.mailbox_id = target.id;
            copied.mailbox_role = target.role;
            copied.mailbox_name = target.name;
        } else {
            copied.mailbox_id = target_mailbox_id;
        }
        self.emails.lock().unwrap().push(copied.clone());
        Box::pin(async move { Ok(copied) })
    }

    fn update_jmap_email_flags<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        unread: Option<bool>,
        flagged: Option<bool>,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        let mut emails = self.emails.lock().unwrap();
        let email = emails
            .iter_mut()
            .find(|email| email.id == message_id)
            .unwrap();
        if let Some(unread) = unread {
            email.unread = unread;
        }
        if let Some(flagged) = flagged {
            email.flagged = flagged;
        }
        let updated = email.clone();
        Box::pin(async move { Ok(updated) })
    }

    fn update_jmap_email_followup_flags<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        update: lpe_storage::JmapEmailFollowupUpdate,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        let mut emails = self.emails.lock().unwrap();
        let email = emails
            .iter_mut()
            .find(|email| email.id == message_id)
            .unwrap();
        if let Some(unread) = update.unread {
            email.unread = unread;
        }
        if let Some(flagged) = update.flagged {
            email.flagged = flagged;
        }
        if let Some(status) = update.followup_flag_status {
            email.followup_flag_status = status.clone();
            email.flagged = status != "none";
        }
        if let Some(icon) = update.followup_icon {
            email.followup_icon = icon;
        }
        if let Some(flags) = update.todo_item_flags {
            email.todo_item_flags = flags;
        }
        if let Some(request) = update.followup_request {
            email.followup_request = request;
        }
        if let Some(start_at) = update.followup_start_at {
            email.followup_start_at = Some(start_at);
        }
        if let Some(due_at) = update.followup_due_at {
            email.followup_due_at = Some(due_at);
        }
        if let Some(completed_at) = update.followup_completed_at {
            email.followup_completed_at = Some(completed_at);
        }
        if let Some(reminder_set) = update.reminder_set {
            email.reminder_set = reminder_set;
        }
        if let Some(reminder_at) = update.reminder_at {
            email.reminder_at = Some(reminder_at);
        }
        if let Some(reminder_dismissed_at) = update.reminder_dismissed_at {
            email.reminder_dismissed_at = Some(reminder_dismissed_at);
        }
        if let Some(store_id) = update.swapped_todo_store_id {
            email.swapped_todo_store_id = Some(store_id);
        }
        if let Some(data) = update.swapped_todo_data {
            email.swapped_todo_data = Some(data);
        }
        if let Some(categories) = update.categories {
            email.categories = categories.clone();
            for state in &mut email.mailbox_states {
                state.categories = categories.clone();
            }
        }
        let updated = email.clone();
        Box::pin(async move { Ok(updated) })
    }

    fn update_jmap_email_content<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        subject: Option<String>,
        body_text: Option<String>,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        let mut emails = self.emails.lock().unwrap();
        let email = emails
            .iter_mut()
            .find(|email| email.id == message_id)
            .unwrap();
        if let Some(subject) = subject {
            email.subject = subject;
        }
        if let Some(body_text) = body_text {
            email.preview = body_text.clone();
            email.body_text = body_text;
        }
        let updated = email.clone();
        Box::pin(async move { Ok(updated) })
    }

    fn delete_jmap_email<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        self.deleted_emails.lock().unwrap().push(message_id);
        self.emails
            .lock()
            .unwrap()
            .retain(|email| email.id != message_id);
        Box::pin(async move { Ok(()) })
    }

    fn delete_jmap_email_from_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        _mailbox_id: Uuid,
        message_id: Uuid,
        audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        if self
            .failed_delete_email_ids
            .lock()
            .unwrap()
            .contains(&message_id)
        {
            return Box::pin(async move { Err(anyhow::anyhow!("forced delete failure")) });
        }
        self.delete_jmap_email(account_id, message_id, audit)
    }

    fn replace_message_recipients<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        to: &'a [SubmittedRecipientInput],
        cc: &'a [SubmittedRecipientInput],
        bcc: &'a [SubmittedRecipientInput],
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        let mut emails = self.emails.lock().unwrap();
        let Some(email) = emails.iter_mut().find(|email| email.id == message_id) else {
            return Box::pin(async move { Err(anyhow::anyhow!("message not found")) });
        };
        email.to = to
            .iter()
            .map(|recipient| JmapEmailAddress {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect();
        email.cc = cc
            .iter()
            .map(|recipient| JmapEmailAddress {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect();
        email.bcc = bcc
            .iter()
            .map(|recipient| JmapEmailAddress {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect();
        Box::pin(async move { Ok(()) })
    }

    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage> {
        self.saved_drafts.lock().unwrap().push(input);
        Box::pin(async move {
            Ok(SavedDraftMessage {
                message_id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
                account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                submitted_by_account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                    .unwrap(),
                draft_mailbox_id: Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
                delivery_status: "draft".to_string(),
            })
        })
    }

    fn submit_draft_message<'a>(
        &'a self,
        account_id: Uuid,
        draft_message_id: Uuid,
        submitted_by_account_id: Uuid,
        source: &'a str,
        audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage> {
        self.submitted_draft_messages
            .lock()
            .unwrap()
            .push(draft_message_id);
        let draft = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .find(|email| email.id == draft_message_id)
            .cloned();
        let Some(draft) = draft else {
            return Box::pin(async move { Err(anyhow::anyhow!("draft message not found")) });
        };
        let input = SubmitMessageInput {
            draft_message_id: Some(draft_message_id),
            account_id,
            submitted_by_account_id,
            source: source.to_string(),
            from_display: draft.from_display,
            from_address: draft.from_address,
            sender_display: draft.sender_display,
            sender_address: draft.sender_address,
            to: draft
                .to
                .into_iter()
                .map(|recipient| SubmittedRecipientInput {
                    address: recipient.address,
                    display_name: recipient.display_name,
                })
                .collect(),
            cc: draft
                .cc
                .into_iter()
                .map(|recipient| SubmittedRecipientInput {
                    address: recipient.address,
                    display_name: recipient.display_name,
                })
                .collect(),
            bcc: draft
                .bcc
                .into_iter()
                .map(|recipient| SubmittedRecipientInput {
                    address: recipient.address,
                    display_name: recipient.display_name,
                })
                .collect(),
            subject: draft.subject,
            body_text: draft.body_text,
            body_html_sanitized: draft.body_html_sanitized,
            internet_message_id: draft.internet_message_id,
            mime_blob_ref: draft.mime_blob_ref,
            size_octets: draft.size_octets,
            unread: Some(draft.unread),
            flagged: Some(draft.flagged),
            attachments: Vec::new(),
        };
        self.submit_message(input, audit)
    }

    fn submit_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage> {
        self.submitted_messages.lock().unwrap().push(input.clone());
        let sent_mailbox = self
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|mailbox| mailbox.role == "sent")
            .cloned();
        let sent_mailbox_id = sent_mailbox
            .as_ref()
            .map(|mailbox| mailbox.id)
            .unwrap_or_else(|| Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap());
        let submitted = SubmittedMessage {
            message_id: Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
            thread_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            account_id: input.account_id,
            submitted_by_account_id: input.submitted_by_account_id,
            sent_mailbox_id,
            outbound_queue_id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
            delivery_status: "queued".to_string(),
        };

        let mut sent = FakeStore::email(
            &submitted.message_id.to_string(),
            &sent_mailbox_id.to_string(),
            "sent",
            &input.subject,
        );
        sent.thread_id = submitted.thread_id;
        sent.mailbox_name = sent_mailbox
            .as_ref()
            .map(|mailbox| mailbox.name.clone())
            .unwrap_or_else(|| "Sent".to_string());
        sent.sent_at = Some("2026-05-07T12:00:00Z".to_string());
        sent.from_address = input.from_address;
        sent.from_display = input.from_display;
        sent.sender_address = input.sender_address;
        sent.sender_display = input.sender_display;
        sent.submitted_by_account_id = input.submitted_by_account_id;
        sent.to = FakeStore::email_addresses(&input.to);
        sent.cc = FakeStore::email_addresses(&input.cc);
        sent.bcc = FakeStore::email_addresses(&input.bcc);
        sent.preview = input.body_text.clone();
        sent.body_text = input.body_text;
        sent.body_html_sanitized = input.body_html_sanitized;
        sent.internet_message_id = input.internet_message_id;
        sent.mime_blob_ref = input.mime_blob_ref;
        sent.size_octets = input.size_octets;
        sent.unread = input.unread.unwrap_or(false);
        sent.flagged = input.flagged.unwrap_or(false);
        sent.has_attachments = !input.attachments.is_empty();
        sent.delivery_status = submitted.delivery_status.clone();
        sent.mailbox_ids = vec![sent_mailbox_id];
        sent.mailbox_states = vec![JmapEmailMailboxState {
            mailbox_id: sent_mailbox_id,
            role: "sent".to_string(),
            name: sent.mailbox_name.clone(),
            modseq: sent.modseq,
            unread: sent.unread,
            flagged: sent.flagged,
            followup_flag_status: sent.followup_flag_status.clone(),
            followup_icon: sent.followup_icon,
            todo_item_flags: sent.todo_item_flags,
            followup_request: sent.followup_request.clone(),
            followup_start_at: sent.followup_start_at.clone(),
            followup_due_at: sent.followup_due_at.clone(),
            followup_completed_at: sent.followup_completed_at.clone(),
            reminder_set: sent.reminder_set,
            reminder_at: sent.reminder_at.clone(),
            reminder_dismissed_at: sent.reminder_dismissed_at.clone(),
            swapped_todo_store_id: sent.swapped_todo_store_id,
            swapped_todo_data: sent.swapped_todo_data.clone(),
            categories: sent.categories.clone(),
            draft: false,
        }];

        let mut emails = self.emails.lock().unwrap();
        if let Some(draft_message_id) = input.draft_message_id {
            emails.retain(|email| email.id != draft_message_id);
        }
        emails.push(sent);

        Box::pin(async move { Ok(submitted) })
    }

    fn cancel_queued_submission<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, CancelSubmissionResult> {
        self.cancelled_submissions.lock().unwrap().push(message_id);
        let result = self
            .submission_cancel_results
            .lock()
            .unwrap()
            .get(&message_id)
            .copied()
            .unwrap_or_else(|| {
                self.emails
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|email| email.id == message_id)
                    .map(|email| match email.delivery_status.as_str() {
                        "queued" | "ready" | "deferred" => CancelSubmissionResult::Cancelled,
                        "cancelled" => CancelSubmissionResult::AlreadyCancelled,
                        "handed_off" | "relayed" | "bounced" | "failed" => {
                            CancelSubmissionResult::NotCancellable
                        }
                        _ => CancelSubmissionResult::NotFound,
                    })
                    .unwrap_or(CancelSubmissionResult::NotFound)
            });
        Box::pin(async move { Ok(result) })
    }
}

#[tokio::test]
async fn fake_store_custom_property_values_survive_restart_style_clone() {
    let store = FakeStore::default();
    let restarted = store.clone();
    let account_id = FakeStore::account().account_id;
    let canonical_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let first_tag = 0x8001_001F;
    let second_tag = 0x8002_0102;

    store
        .upsert_mapi_custom_property_values(
            account_id,
            MapiCustomPropertyObjectKind::Contact,
            canonical_id,
            &[
                MapiCustomPropertyValue {
                    property_tag: first_tag,
                    property_type: 0x001F,
                    property_value: utf16z("persisted custom value"),
                },
                MapiCustomPropertyValue {
                    property_tag: second_tag,
                    property_type: 0x0102,
                    property_value: vec![3, 0, 0xAA, 0xBB, 0xCC],
                },
            ],
        )
        .await
        .unwrap();

    let fetched = restarted
        .fetch_mapi_custom_property_values(
            account_id,
            MapiCustomPropertyObjectKind::Contact,
            canonical_id,
            &[second_tag, first_tag],
        )
        .await
        .unwrap();
    assert_eq!(fetched.len(), 2);
    assert_eq!(fetched[0].property_tag, first_tag);
    assert_eq!(fetched[0].property_value, utf16z("persisted custom value"));
    assert_eq!(fetched[1].property_tag, second_tag);
    assert_eq!(fetched[1].property_value, vec![3, 0, 0xAA, 0xBB, 0xCC]);

    restarted
        .delete_mapi_custom_property_values(
            account_id,
            MapiCustomPropertyObjectKind::Contact,
            canonical_id,
            &[first_tag],
        )
        .await
        .unwrap();
    let fetched = store
        .fetch_mapi_custom_property_values(
            account_id,
            MapiCustomPropertyObjectKind::Contact,
            canonical_id,
            &[first_tag, second_tag],
        )
        .await
        .unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].property_tag, second_tag);
}

#[tokio::test]
async fn fake_store_all_custom_property_values_are_scoped_to_one_mapi_object() {
    let store = FakeStore::default();
    let restarted = store.clone();
    let account_id = FakeStore::account().account_id;
    let source_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    let other_id = Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
    store
        .upsert_mapi_custom_property_values(
            account_id,
            MapiCustomPropertyObjectKind::Message,
            source_id,
            &[
                MapiCustomPropertyValue {
                    property_tag: 0x8002_0102,
                    property_type: 0x0102,
                    property_value: vec![2, 0, 0x44, 0x55],
                },
                MapiCustomPropertyValue {
                    property_tag: 0x8001_001F,
                    property_type: 0x001F,
                    property_value: utf16z("source opaque"),
                },
            ],
        )
        .await
        .unwrap();
    store
        .upsert_mapi_custom_property_values(
            account_id,
            MapiCustomPropertyObjectKind::Message,
            other_id,
            &[MapiCustomPropertyValue {
                property_tag: 0x8003_001F,
                property_type: 0x001F,
                property_value: utf16z("other opaque"),
            }],
        )
        .await
        .unwrap();

    let fetched = restarted
        .fetch_all_mapi_custom_property_values(
            account_id,
            MapiCustomPropertyObjectKind::Message,
            source_id,
        )
        .await
        .unwrap();

    assert_eq!(fetched.len(), 2);
    assert_eq!(fetched[0].property_tag, 0x8001_001F);
    assert_eq!(fetched[0].property_value, utf16z("source opaque"));
    assert_eq!(fetched[1].property_tag, 0x8002_0102);
    assert_eq!(fetched[1].property_value, vec![2, 0, 0x44, 0x55]);
}

fn bearer_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer token"),
    );
    headers
}

fn rpc_proxy_conn_a1_request_body(receive_window_size: u32) -> Vec<u8> {
    let mut body = Vec::with_capacity(76);
    body.extend_from_slice(&[0x05, 0x00, 0x14, 0x03, 0x10, 0x00, 0x00, 0x00]);
    body.extend_from_slice(&76u16.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&4u16.to_le_bytes());
    body.extend_from_slice(&6u32.to_le_bytes());
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(&3u32.to_le_bytes());
    body.extend_from_slice(&[0x11; 16]);
    body.extend_from_slice(&3u32.to_le_bytes());
    body.extend_from_slice(&[0x22; 16]);
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&receive_window_size.to_le_bytes());
    body
}

fn mapi_headers(request_type: &str) -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/mapi-http"),
    );
    insert_mapi_content_length(&mut headers);
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_str(&mapi_request_id()).unwrap(),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_str(&mapi_client_info()).unwrap(),
    );
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers
}

fn insert_mapi_content_length(headers: &mut HeaderMap) {
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_static("0"),
    );
}

fn renew_mapi_request_id(headers: &mut HeaderMap) {
    headers.insert(
        "x-requestid",
        HeaderValue::from_str(&mapi_request_id()).unwrap(),
    );
}

fn mapi_request_id() -> String {
    format!(
        "{{11111111-2222-3333-4444-555555555555}}:{}",
        MAPI_TEST_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn mapi_client_info() -> String {
    format!(
        "{{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}}:{}",
        MAPI_TEST_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn mapi_headers_without_content_type(request_type: &str) -> HeaderMap {
    let mut headers = bearer_headers();
    insert_mapi_content_length(&mut headers);
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_str(&mapi_request_id()).unwrap(),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_str(&mapi_client_info()).unwrap(),
    );
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers
}

fn mapi_headers_with_content_type(request_type: &str, content_type: &'static str) -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    insert_mapi_content_length(&mut headers);
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_str(&mapi_request_id()).unwrap(),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_str(&mapi_client_info()).unwrap(),
    );
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers
}

fn mapi_headers_without_request_id(request_type: &str) -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/mapi-http"),
    );
    insert_mapi_content_length(&mut headers);
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_str(&mapi_client_info()).unwrap(),
    );
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers
}

fn mapi_headers_without_request_type() -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/mapi-http"),
    );
    insert_mapi_content_length(&mut headers);
    headers.insert(
        "x-requestid",
        HeaderValue::from_str(&mapi_request_id()).unwrap(),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_str(&mapi_client_info()).unwrap(),
    );
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers
}

fn mapi_headers_without_client_info(request_type: &str) -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/mapi-http"),
    );
    insert_mapi_content_length(&mut headers);
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_str(&mapi_request_id()).unwrap(),
    );
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers
}

fn mapi_headers_with_request_id(request_type: &str, request_id: &'static str) -> HeaderMap {
    let mut headers = mapi_headers(request_type);
    headers.insert("x-requestid", HeaderValue::from_static(request_id));
    headers
}

fn mapi_headers_with_client_info(request_type: &str, client_info: &'static str) -> HeaderMap {
    let mut headers = mapi_headers(request_type);
    headers.insert("x-clientinfo", HeaderValue::from_static(client_info));
    headers
}

fn mapi_headers_without_host(request_type: &str) -> HeaderMap {
    let mut headers = mapi_headers(request_type);
    headers.remove("host");
    headers
}

fn mapi_headers_without_content_length(request_type: &str) -> HeaderMap {
    let mut headers = mapi_headers(request_type);
    headers.remove(axum::http::header::CONTENT_LENGTH);
    headers
}

fn mapi_headers_with_content_length(request_type: &str, content_length: &'static str) -> HeaderMap {
    let mut headers = mapi_headers(request_type);
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_static(content_length),
    );
    headers
}

fn execute_body(rop_buffer: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&(rop_buffer.len() as u32).to_le_bytes());
    body.extend_from_slice(rop_buffer);
    body.extend_from_slice(&65536u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body
}

fn mapi_submit_execute_body(subject: &str) -> Vec<u8> {
    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, subject);
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Transport gate body");
    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 2, &property_values);
    append_rop_modify_recipients(&mut rops, 2, &[(1, 0x01, to_row.as_slice())]);
    append_rop_submit_message(&mut rops, 2);
    execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]))
}

fn rop_buffer(rops: &[u8], handles: &[u32]) -> Vec<u8> {
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&(rops.len() as u16).to_le_bytes());
    buffer.extend_from_slice(rops);
    for handle in handles {
        buffer.extend_from_slice(&handle.to_le_bytes());
    }
    buffer
}

fn rca_wrapped_private_logon_execute_body(mailbox: &str, client: &str) -> Vec<u8> {
    let mut rops = Vec::new();
    rops.push(0xFE);
    rops.push(0x01);
    rops.push(0x00);
    rops.push(0x41);
    rops.extend_from_slice(&0x0100_040Cu32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&((mailbox.len() + 1) as u16).to_le_bytes());
    rops.extend_from_slice(&8u32.to_le_bytes());
    rops.extend_from_slice(mailbox.as_bytes());
    rops.push(0);
    rops.extend_from_slice(&0x001Fu16.to_le_bytes());
    rops.extend_from_slice(client.as_bytes());

    let mut payload = Vec::new();
    payload.extend_from_slice(&((rops.len() + 2) as u16).to_le_bytes());
    payload.extend_from_slice(&rops);
    payload.extend_from_slice(&u32::MAX.to_le_bytes());

    let mut rpc_header_ext = Vec::new();
    rpc_header_ext.extend_from_slice(&0u16.to_le_bytes());
    rpc_header_ext.extend_from_slice(&0x0004u16.to_le_bytes());
    rpc_header_ext.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    rpc_header_ext.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    rpc_header_ext.extend_from_slice(&payload);

    let mut body = Vec::new();
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&(rpc_header_ext.len() as u32).to_le_bytes());
    body.extend_from_slice(&rpc_header_ext);
    body.extend_from_slice(&0x8007u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body
}

fn test_account_principal() -> AccountPrincipal {
    let account = FakeStore::account();
    AccountPrincipal {
        tenant_id: account.tenant_id,
        account_id: account.account_id,
        email: account.email,
        display_name: account.display_name,
        quota_mb: None,
        quota_used_octets: None,
    }
}

fn rpc_proxy_bootstrap_logon_execute_rop(mailbox: &str) -> Vec<u8> {
    let legacy_dn = format!("/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={mailbox}\0");
    let mut rops = vec![0xFE, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    rops.extend_from_slice(legacy_dn.as_bytes());
    rpc_proxy_wrapped_rop_buffer(&rops, &[u32::MAX])
}

fn rpc_proxy_wrapped_rop_buffer(rops: &[u8], handles: &[u32]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&((rops.len() + 2) as u16).to_le_bytes());
    payload.extend_from_slice(rops);
    for handle in handles {
        payload.extend_from_slice(&handle.to_le_bytes());
    }

    let mut rpc_header_ext = Vec::new();
    rpc_header_ext.extend_from_slice(&0u16.to_le_bytes());
    rpc_header_ext.extend_from_slice(&0x0004u16.to_le_bytes());
    rpc_header_ext.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    rpc_header_ext.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    rpc_header_ext.extend_from_slice(&payload);
    rpc_header_ext
}

fn resolve_names_request(search_address: &str, columns: &[u32]) -> Vec<u8> {
    let mut request = Vec::new();
    request.extend_from_slice(&0u32.to_le_bytes());
    request.push(0xFF);
    request.extend_from_slice(&[0; 24]);
    request.extend_from_slice(&1252u32.to_le_bytes());
    request.extend_from_slice(&0x0409u32.to_le_bytes());
    request.extend_from_slice(&0x0409u32.to_le_bytes());
    request.push(0xFF);
    request.extend_from_slice(&(columns.len() as u32).to_le_bytes());
    for column in columns {
        request.extend_from_slice(&column.to_le_bytes());
    }
    request.push(0xFF);
    request.extend_from_slice(&1u32.to_le_bytes());
    let unresolved_name = utf16z(&format!("=SMTP:{search_address}"));
    request.extend_from_slice(&(unresolved_name.len() as u16).to_le_bytes());
    request.extend_from_slice(&unresolved_name);
    request.extend_from_slice(&0u32.to_le_bytes());
    request
}

async fn response_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn decoded_mime_content(response: &str) -> String {
    let encoded = response
        .split("<t:MimeContent CharacterSet=\"UTF-8\">")
        .nth(1)
        .and_then(|value| value.split("</t:MimeContent>").next())
        .unwrap();
    String::from_utf8(BASE64_STANDARD.decode(encoded.as_bytes()).unwrap()).unwrap()
}

async fn response_bytes(response: axum::response::Response) -> Vec<u8> {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    strip_mapi_http_envelope(bytes)
}

async fn raw_response_bytes(response: axum::response::Response) -> Vec<u8> {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

fn mapi_cookie_header(response: &axum::response::Response) -> String {
    response
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter_map(|value| value.split(';').next())
        .collect::<Vec<_>>()
        .join("; ")
}

fn mapi_cookie_header_with_mismatched_sequence(response: &axum::response::Response) -> String {
    mapi_cookie_header(response)
        .split("; ")
        .map(|part| {
            if part.starts_with("MapiSequence=") {
                "MapiSequence=00000000-0000-0000-0000-000000000000".to_string()
            } else {
                part.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

async fn nspi_bound_headers(service: &ExchangeService<FakeStore>, request_type: &str) -> HeaderMap {
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();
    let mut headers = mapi_headers(request_type);
    headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&bind)).unwrap(),
    );
    headers
}

fn parse_attachment_reference(value: &str) -> Option<(Uuid, Uuid)> {
    let value = value.trim();
    let rest = value.strip_prefix("attachment:")?;
    let (message_id, attachment_id) = rest.split_once(':')?;
    Some((
        Uuid::parse_str(message_id).ok()?,
        Uuid::parse_str(attachment_id).ok()?,
    ))
}

fn strip_mapi_http_envelope(bytes: Vec<u8>) -> Vec<u8> {
    if !bytes.starts_with(b"PROCESSING\r\nDONE\r\n") {
        return bytes;
    }
    let Some(offset) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
        return bytes;
    };
    bytes[offset + 4..].to_vec()
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn notification_detail_strings(mut bytes: &[u8]) -> Vec<String> {
    let mut values = Vec::new();
    while bytes.len() >= 2 {
        let size = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
        bytes = &bytes[2..];
        if bytes.len() < size {
            break;
        }
        values.push(String::from_utf8_lossy(&bytes[..size]).into_owned());
        bytes = &bytes[size..];
    }
    values
}

fn mapi_sync_manifest_counts(bytes: &[u8]) -> Option<(u32, u32)> {
    let change_marker = 0x4012_0003u32.to_le_bytes();
    let message_marker = 0x4015_0003u32.to_le_bytes();
    let state_marker = 0x403A_0003u32.to_le_bytes();
    let end_marker = 0x4014_0003u32.to_le_bytes();
    let mut folder_count = 0;
    let mut message_count = 0;
    let mut offset = 0;
    while offset + 4 <= bytes.len() {
        if bytes[offset..offset + 4] == change_marker {
            let next_change = bytes[offset + 4..]
                .windows(4)
                .position(|window| {
                    window == change_marker || window == state_marker || window == end_marker
                })
                .map(|position| offset + 4 + position)
                .unwrap_or(bytes.len());
            if bytes[offset + 4..next_change]
                .windows(4)
                .any(|window| window == message_marker)
            {
                message_count += 1;
            } else {
                folder_count += 1;
            }
            offset = next_change;
            continue;
        }
        offset += 1;
    }
    if folder_count == 0 && message_count == 0 {
        None
    } else {
        Some((folder_count, message_count))
    }
}

fn assert_content_final_state_includes(bytes: &[u8], message_ids: &[Uuid], change_numbers: &[u64]) {
    let message_counters = message_ids
        .iter()
        .map(mapi_message_global_counter)
        .collect::<Vec<_>>();
    assert_content_final_state_includes_counters(bytes, &message_counters, change_numbers);
}

fn assert_content_final_state_includes_counters(
    bytes: &[u8],
    message_counters: &[u64],
    change_numbers: &[u64],
) {
    let idset_given = mapi_binary_property_value(bytes, META_TAG_IDSET_GIVEN);
    for message_counter in message_counters {
        assert!(
            strict_replguid_globset_contains_counter(idset_given, &globcnt_bytes(*message_counter))
                .unwrap(),
            "final MetaTagIdsetGiven missing message counter {message_counter}"
        );
    }

    for tag in [META_TAG_CNSET_SEEN, META_TAG_CNSET_READ] {
        let cnset = mapi_binary_property_value(bytes, tag);
        for change_number in change_numbers {
            assert!(
                strict_replguid_globset_contains_counter(cnset, &globcnt_bytes(*change_number))
                    .unwrap(),
                "final content CNSET 0x{tag:08x} missing change {change_number}"
            );
        }
    }
}

fn mapi_binary_property_value(bytes: &[u8], property_tag: u32) -> &[u8] {
    let tag = property_tag.to_le_bytes();
    let offset = bytes
        .windows(tag.len())
        .position(|window| window == tag)
        .expect("MAPI binary property is present");
    let length = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
    &bytes[offset + 8..offset + 8 + length]
}

fn additional_ren_entry_ids_ex_entries(value: &[u8]) -> Vec<(u16, u64)> {
    let mut offset = 0;
    let mut entries = Vec::new();
    loop {
        let persist_id = u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
        let data_size = u16::from_le_bytes(value[offset + 2..offset + 4].try_into().unwrap());
        offset += 4;
        if persist_id == 0 {
            break;
        }

        let block_end = offset + data_size as usize;
        let mut folder_id = None;
        while offset < block_end {
            let element_id = u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
            let element_size =
                u16::from_le_bytes(value[offset + 2..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if element_id == 0 {
                break;
            }
            if element_id == 0x0001 {
                folder_id = crate::mapi::identity::object_id_from_folder_identifier_bytes(
                    &value[offset..offset + element_size],
                );
            }
            offset += element_size;
        }
        offset = block_end;
        entries.push((persist_id, folder_id.expect("entry id element")));
    }
    entries
}

const FX_INCR_SYNC_CHG: u32 = 0x4012_0003;
const FX_INCR_SYNC_DEL: u32 = 0x4013_0003;
const FX_INCR_SYNC_END: u32 = 0x4014_0003;
const FX_INCR_SYNC_MESSAGE: u32 = 0x4015_0003;
const FX_INCR_SYNC_READ: u32 = 0x402F_0003;
const FX_INCR_SYNC_STATE_BEGIN: u32 = 0x403A_0003;
const FX_INCR_SYNC_STATE_END: u32 = 0x403B_0003;
const FX_INCR_SYNC_PROGRESS_MODE: u32 = 0x4074_000B;
const FX_INCR_SYNC_PROGRESS_PER_MSG: u32 = 0x4075_000B;
const FX_NEW_ATTACH: u32 = 0x4000_0003;
const FX_START_EMBED: u32 = 0x4001_0003;
const FX_END_EMBED: u32 = 0x4002_0003;
const FX_START_RECIP: u32 = 0x4003_0003;
const FX_END_TO_RECIP: u32 = 0x4004_0003;
const FX_END_ATTACH: u32 = 0x400E_0003;
const PID_TAG_SUBJECT_W: u32 = 0x0037_001F;
const PID_TAG_MESSAGE_CLASS_W: u32 = 0x001A_001F;
const PID_TAG_NORMALIZED_SUBJECT_A: u32 = 0x0E1D_001E;
const PID_TAG_DISPLAY_NAME_W: u32 = 0x3001_001F;
const PID_TAG_EMAIL_ADDRESS_W: u32 = 0x3003_001F;
const PID_TAG_FOLDER_TYPE: u32 = 0x3601_0003;
const PID_TAG_CONTENT_COUNT: u32 = 0x3602_0003;
const PID_TAG_CONTENT_UNREAD_COUNT: u32 = 0x3603_0003;
const PID_TAG_SUBFOLDERS: u32 = 0x360A_000B;
const PID_TAG_CONTAINER_CLASS_W: u32 = 0x3613_001F;
const PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX: u32 = 0x36D9_0102;
const PID_TAG_LAST_MODIFICATION_TIME: u32 = 0x3008_0040;
const PID_TAG_LOCAL_COMMIT_TIME_MAX: u32 = 0x670A_0040;
const PID_TAG_DELETED_COUNT_TOTAL: u32 = 0x670B_0003;
const PID_TAG_MESSAGE_FLAGS: u32 = 0x0E07_0003;
const PID_TAG_MESSAGE_SIZE: u32 = 0x0E08_0003;
const PID_TAG_FLAG_STATUS: u32 = 0x1090_0003;
const PID_TAG_RECIPIENT_TYPE: u32 = 0x0C15_0003;
const PID_TAG_ATTACH_SIZE: u32 = 0x0E20_0003;
const PID_TAG_ATTACH_NUM: u32 = 0x0E21_0003;
const PID_TAG_ENTRY_ID: u32 = 0x0FFF_0102;
const PID_TAG_ASSOCIATED: u32 = 0x67AA_000B;
const PID_TAG_SOURCE_KEY: u32 = 0x65E0_0102;
const PID_TAG_PARENT_SOURCE_KEY: u32 = 0x65E1_0102;
const PID_TAG_CHANGE_KEY: u32 = 0x65E2_0102;
const PID_TAG_PREDECESSOR_CHANGE_LIST: u32 = 0x65E3_0102;
const PID_TAG_SEARCH_FOLDER_DEFINITION: u32 = 0x6845_0102;
const PID_TAG_WLINK_GROUP_HEADER_ID: u32 = 0x6842_0048;
const PID_TAG_WLINK_TYPE: u32 = 0x6849_0003;
const PID_TAG_WLINK_ORDINAL: u32 = 0x684B_0102;
const PID_TAG_WLINK_ENTRY_ID: u32 = 0x684C_0102;
const PID_TAG_WLINK_GROUP_CLSID: u32 = 0x6850_0048;
const PID_TAG_WLINK_GROUP_NAME_W: u32 = 0x6851_001F;
const PID_TAG_VIEW_DESCRIPTOR_BINARY: u32 = 0x7001_0102;
const PID_TAG_VIEW_DESCRIPTOR_STRINGS_W: u32 = 0x7002_001F;
const PID_TAG_VIEW_DESCRIPTOR_NAME_W: u32 = 0x7006_001F;
const PID_TAG_VIEW_DESCRIPTOR_VERSION: u32 = 0x683A_0003;
const PID_TAG_ATTACH_ENCODING: u32 = 0x3702_0102;
const PID_TAG_ATTACH_FILENAME_W: u32 = 0x3704_001F;
const PID_TAG_ATTACH_METHOD: u32 = 0x3705_0003;
const PID_TAG_ATTACH_LONG_FILENAME_W: u32 = 0x3707_001F;
const PID_TAG_ATTACH_RENDERING: u32 = 0x3709_0102;
const PID_TAG_RENDERING_POSITION: u32 = 0x370B_0003;
const PID_TAG_ATTACH_MIME_TAG_W: u32 = 0x370E_001F;
const PID_TAG_ATTACH_FLAGS: u32 = 0x3714_0003;
const PID_TAG_ATTACHMENT_HIDDEN: u32 = 0x7FFE_000B;
const PID_TAG_FOLDER_ID: u32 = 0x6748_0014;
const PID_TAG_PARENT_FOLDER_ID: u32 = 0x6749_0014;
const PID_TAG_MID: u32 = 0x674A_0014;
const PID_TAG_CHANGE_NUMBER: u32 = 0x67A4_0014;
const PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W: u32 = 0x36E5_001F;
const OUTLOOK_IPM_HIERARCHY_FOLDER_COUNT: u32 = 19;
const OUTLOOK_IPM_HIERARCHY_TABLE_FOLDER_COUNT: u32 = 15;
const PRIVATE_LOGON_SPECIAL_FOLDER_ID_COUNT: usize = 13;
const META_TAG_IDSET_GIVEN: u32 = 0x4017_0003;
const META_TAG_IDSET_GIVEN_BINARY: u32 = 0x4017_0102;
const META_TAG_IDSET_DELETED: u32 = 0x4018_0102;
const META_TAG_IDSET_READ: u32 = 0x402D_0102;
const META_TAG_IDSET_UNREAD: u32 = 0x402E_0102;
const META_TAG_CNSET_SEEN: u32 = 0x6796_0102;
const META_TAG_CNSET_SEEN_FAI: u32 = 0x67DA_0102;
const META_TAG_CNSET_READ: u32 = 0x67D2_0102;

#[derive(Debug)]
struct StrictHierarchySyncStream {
    folder_changes: Vec<StrictHierarchyFolderChange>,
    idset_given: Vec<u8>,
    cnset_seen: Vec<u8>,
}

#[derive(Debug)]
struct StrictHierarchyFolderChange {
    source_key: Vec<u8>,
    parent_source_key: Vec<u8>,
    change_key: Vec<u8>,
    display_name: String,
    container_class: Option<String>,
    default_post_message_class: Option<String>,
    folder_id: Option<u64>,
    parent_folder_id: Option<u64>,
    folder_type: Option<u32>,
    content_count: Option<u32>,
    content_unread_count: Option<u32>,
    local_commit_time_max: Option<u64>,
    deleted_count_total: Option<u32>,
}

#[derive(Debug, Default)]
struct StrictHierarchyFolderBuilder {
    tags: Vec<u32>,
    source_key: Option<Vec<u8>>,
    parent_source_key: Option<Vec<u8>>,
    change_key: Option<Vec<u8>>,
    display_name: Option<String>,
    container_class: Option<String>,
    default_post_message_class: Option<String>,
    folder_id: Option<u64>,
    parent_folder_id: Option<u64>,
    folder_type: Option<u32>,
    content_count: Option<u32>,
    content_unread_count: Option<u32>,
    local_commit_time_max: Option<u64>,
    deleted_count_total: Option<u32>,
}

struct StrictFastTransferProperty {
    tag: u32,
    value: Vec<u8>,
    next_offset: usize,
}

fn strict_hierarchy_sync_transfer_from_response(
    response_rops: &[u8],
) -> Result<StrictHierarchySyncStream, String> {
    let chunks = mapi_fast_transfer_chunks(response_rops);
    if chunks.len() != 1 {
        return Err(format!(
            "expected one completed FastTransfer chunk, got {}",
            chunks.len()
        ));
    }
    if chunks[0].0 != 0x0003 {
        return Err(format!(
            "expected completed FastTransfer status 0x0003, got 0x{:04x}",
            chunks[0].0
        ));
    }
    strict_decode_hierarchy_sync_stream(&chunks[0].1)
}

fn strict_decode_hierarchy_sync_stream(bytes: &[u8]) -> Result<StrictHierarchySyncStream, String> {
    let mut offset = 0;
    let mut current_folder: Option<StrictHierarchyFolderBuilder> = None;
    let mut folder_changes = Vec::new();
    let mut seen_source_keys: Vec<Vec<u8>> = vec![
        mapi_mailstore::source_key_for_store_id(crate::mapi::identity::ROOT_FOLDER_ID),
        mapi_mailstore::source_key_for_store_id(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID),
    ];
    let mut in_state = false;
    let mut state_closed = false;
    let mut idset_given = None;
    let mut cnset_seen = None;

    while offset < bytes.len() {
        let tag = read_strict_u32(bytes, offset)?;
        if strict_hierarchy_marker(tag) {
            match tag {
                FX_INCR_SYNC_CHG => {
                    if in_state || state_closed {
                        return Err(
                            "folderChange marker appears after final ICS state starts".into()
                        );
                    }
                    if let Some(folder) = current_folder.take() {
                        strict_finish_folder_change(
                            folder,
                            &mut seen_source_keys,
                            &mut folder_changes,
                        )?;
                    }
                    current_folder = Some(StrictHierarchyFolderBuilder::default());
                }
                FX_INCR_SYNC_STATE_BEGIN => {
                    if let Some(folder) = current_folder.take() {
                        strict_finish_folder_change(
                            folder,
                            &mut seen_source_keys,
                            &mut folder_changes,
                        )?;
                    }
                    if in_state || state_closed {
                        return Err("duplicate final ICS state boundary".into());
                    }
                    in_state = true;
                }
                FX_INCR_SYNC_STATE_END => {
                    if !in_state {
                        return Err("IncrSyncStateEnd without IncrSyncStateBegin".into());
                    }
                    if idset_given.is_none() || cnset_seen.is_none() {
                        return Err("final ICS state is missing hierarchy IDSET or CNSET".into());
                    }
                    in_state = false;
                    state_closed = true;
                }
                FX_INCR_SYNC_END => {
                    if current_folder.is_some() {
                        return Err("IncrSyncEnd appears inside an open folderChange".into());
                    }
                    if !state_closed {
                        return Err("IncrSyncEnd appears before final ICS state is complete".into());
                    }
                    offset += 4;
                    if offset != bytes.len() {
                        return Err("trailing bytes after IncrSyncEnd".into());
                    }
                    break;
                }
                _ => unreachable!(),
            }
            offset += 4;
            continue;
        }

        let property = strict_parse_fast_transfer_property(bytes, offset)?;
        offset = property.next_offset;
        if let Some(folder) = current_folder.as_mut() {
            strict_record_folder_property(folder, property)?;
        } else if in_state {
            match property.tag {
                META_TAG_IDSET_GIVEN | META_TAG_IDSET_GIVEN_BINARY => {
                    if idset_given.replace(property.value).is_some() {
                        return Err("duplicate MetaTagIdsetGiven in final ICS state".into());
                    }
                }
                META_TAG_CNSET_SEEN => {
                    if cnset_seen.replace(property.value).is_some() {
                        return Err("duplicate MetaTagCnsetSeen in final ICS state".into());
                    }
                }
                tag => {
                    return Err(format!(
                        "unexpected property 0x{tag:08x} in hierarchy final ICS state"
                    ));
                }
            }
        } else {
            return Err(format!(
                "property 0x{:08x} appears outside folderChange or final state",
                property.tag
            ));
        }
    }

    if offset != bytes.len() {
        return Err("FastTransfer stream ended on a partial atom".into());
    }
    if folder_changes.is_empty() {
        return Err("hierarchy sync stream contained no folderChange rows".into());
    }
    let idset_given = idset_given.ok_or("missing MetaTagIdsetGiven")?;
    let cnset_seen = cnset_seen.ok_or("missing MetaTagCnsetSeen")?;
    strict_validate_replguid_globset(&idset_given)?;
    strict_validate_replguid_globset(&cnset_seen)?;
    for folder in &folder_changes {
        strict_validate_source_or_change_key(&folder.source_key)?;
        strict_validate_source_or_change_key(&folder.change_key)?;
        if !strict_replguid_globset_contains_counter(&idset_given, &folder.source_key[16..22])? {
            return Err(format!(
                "final MetaTagIdsetGiven does not include folder {}",
                folder.display_name
            ));
        }
        if !strict_replguid_globset_contains_counter(&cnset_seen, &folder.change_key[16..22])? {
            return Err(format!(
                "final MetaTagCnsetSeen does not include folder {} change key",
                folder.display_name
            ));
        }
    }

    Ok(StrictHierarchySyncStream {
        folder_changes,
        idset_given,
        cnset_seen,
    })
}

fn strict_hierarchy_marker(tag: u32) -> bool {
    matches!(
        tag,
        FX_INCR_SYNC_CHG | FX_INCR_SYNC_STATE_BEGIN | FX_INCR_SYNC_STATE_END | FX_INCR_SYNC_END
    )
}

fn strict_parse_fast_transfer_property(
    bytes: &[u8],
    offset: usize,
) -> Result<StrictFastTransferProperty, String> {
    let tag = read_strict_u32(bytes, offset)?;
    let property_type = tag & 0x0000_FFFF;
    let value_start = offset + 4;
    let (value_start, value_len) = match property_type {
        _ if tag == META_TAG_IDSET_GIVEN => {
            let len = read_strict_u32(bytes, value_start)? as usize;
            (value_start + 4, len)
        }
        0x0002 => (value_start, 2),
        0x0003 => (value_start, 4),
        0x000B => {
            let value = read_strict_slice(bytes, value_start, 2)?;
            if value != [0, 0] && value != [1, 0] {
                return Err(format!(
                    "PtypBoolean property 0x{tag:08x} has invalid FastTransfer value"
                ));
            }
            (value_start, 2)
        }
        0x0014 | 0x0040 => (value_start, 8),
        0x0048 => (value_start, 16),
        0x001E | 0x001F | 0x0102 => {
            let len = read_strict_u32(bytes, value_start)? as usize;
            let value_start = value_start + 4;
            if property_type == 0x001E {
                let value = read_strict_slice(bytes, value_start, len)?;
                if value.is_empty() || value.last() != Some(&0) {
                    return Err(format!(
                        "PtypString8 property 0x{tag:08x} is not null-terminated"
                    ));
                }
            }
            if property_type == 0x001F {
                let value = read_strict_slice(bytes, value_start, len)?;
                if value.len() < 2 || value.len() % 2 != 0 || value[value.len() - 2..] != [0, 0] {
                    return Err(format!("PtypString property 0x{tag:08x} is not UTF-16Z"));
                }
            }
            (value_start, len)
        }
        0x101F => {
            let count = read_strict_u32(bytes, value_start)? as usize;
            let mut cursor = value_start + 4;
            for _ in 0..count {
                let len = read_strict_u32(bytes, cursor)? as usize;
                cursor += 4;
                let value = read_strict_slice(bytes, cursor, len)?;
                if value.len() < 2 || value.len() % 2 != 0 || value[value.len() - 2..] != [0, 0] {
                    return Err(format!(
                        "PtypMultipleString property 0x{tag:08x} contains non UTF-16Z value"
                    ));
                }
                cursor += len;
            }
            (value_start, cursor - value_start)
        }
        _ => {
            return Err(format!(
                "unsupported FastTransfer property type in 0x{tag:08x}"
            ))
        }
    };
    let value = read_strict_slice(bytes, value_start, value_len)?.to_vec();
    Ok(StrictFastTransferProperty {
        tag,
        value,
        next_offset: value_start + value_len,
    })
}

fn strict_record_folder_property(
    folder: &mut StrictHierarchyFolderBuilder,
    property: StrictFastTransferProperty,
) -> Result<(), String> {
    if property.tag == PID_TAG_MID {
        return Err("message change property appears inside hierarchy folderChange".into());
    }
    if folder.tags.contains(&property.tag) {
        return Err(format!(
            "duplicate property 0x{:08x} inside folderChange",
            property.tag
        ));
    }
    folder.tags.push(property.tag);
    match property.tag {
        PID_TAG_PARENT_SOURCE_KEY => folder.parent_source_key = Some(property.value),
        PID_TAG_SOURCE_KEY => folder.source_key = Some(property.value),
        PID_TAG_CHANGE_KEY => folder.change_key = Some(property.value),
        PID_TAG_DISPLAY_NAME_W => {
            folder.display_name = Some(strict_decode_utf16z(&property.value)?)
        }
        PID_TAG_FOLDER_ID => {
            folder.folder_id = Some(strict_decode_object_id_property(&property)?);
        }
        PID_TAG_PARENT_FOLDER_ID => {
            folder.parent_folder_id = Some(strict_decode_object_id_property(&property)?);
        }
        PID_TAG_FOLDER_TYPE => {
            folder.folder_type = Some(strict_decode_u32_property(&property)?);
        }
        PID_TAG_CONTENT_COUNT => {
            folder.content_count = Some(strict_decode_u32_property(&property)?);
        }
        PID_TAG_CONTENT_UNREAD_COUNT => {
            folder.content_unread_count = Some(strict_decode_u32_property(&property)?);
        }
        PID_TAG_LOCAL_COMMIT_TIME_MAX => {
            folder.local_commit_time_max = Some(strict_decode_u64_property(&property)?);
        }
        PID_TAG_DELETED_COUNT_TOTAL => {
            folder.deleted_count_total = Some(strict_decode_u32_property(&property)?);
        }
        PID_TAG_SUBFOLDERS => {
            if property.value.len() != 2 {
                return Err("PidTagSubfolders was not encoded as a two-byte PtypBoolean".into());
            }
        }
        PID_TAG_CONTAINER_CLASS_W => {
            folder.container_class = Some(strict_decode_utf16z(&property.value)?);
        }
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            folder.default_post_message_class = Some(strict_decode_utf16z(&property.value)?);
        }
        _ => {}
    }
    Ok(())
}

fn strict_decode_u32_property(property: &StrictFastTransferProperty) -> Result<u32, String> {
    if property.value.len() != 4 {
        return Err(format!(
            "property 0x{:08x} was not encoded as a four-byte integer",
            property.tag
        ));
    }
    Ok(u32::from_le_bytes(
        property.value.as_slice().try_into().unwrap(),
    ))
}

fn strict_decode_i32_property(property: &StrictFastTransferProperty) -> Result<i32, String> {
    if property.value.len() != 4 {
        return Err(format!(
            "property 0x{:08x} was not encoded as a four-byte integer",
            property.tag
        ));
    }
    Ok(i32::from_le_bytes(
        property.value.as_slice().try_into().unwrap(),
    ))
}

fn strict_decode_u64_property(property: &StrictFastTransferProperty) -> Result<u64, String> {
    if property.value.len() != 8 {
        return Err(format!(
            "property 0x{:08x} was not encoded as an eight-byte integer",
            property.tag
        ));
    }
    Ok(u64::from_le_bytes(
        property.value.as_slice().try_into().unwrap(),
    ))
}

fn strict_decode_object_id_property(property: &StrictFastTransferProperty) -> Result<u64, String> {
    if property.value.len() != 8 {
        return Err(format!(
            "property 0x{:08x} was not encoded as an eight-byte object id",
            property.tag
        ));
    }
    crate::mapi::identity::object_id_from_wire_id(&property.value)
        .or_else(|| {
            Some(u64::from_le_bytes(
                property.value.as_slice().try_into().unwrap(),
            ))
        })
        .ok_or_else(|| format!("property 0x{:08x} had an invalid object id", property.tag))
}

fn strict_decode_change_number_property(
    property: &StrictFastTransferProperty,
) -> Result<u64, String> {
    let value = strict_decode_object_id_property(property)?;
    crate::mapi::identity::global_counter_from_store_id(value).ok_or_else(|| {
        format!(
            "property 0x{:08x} had an invalid change number",
            property.tag
        )
    })
}

fn strict_finish_folder_change(
    folder: StrictHierarchyFolderBuilder,
    seen_source_keys: &mut Vec<Vec<u8>>,
    folder_changes: &mut Vec<StrictHierarchyFolderChange>,
) -> Result<(), String> {
    let required_prefix = [
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
    ];
    if folder.tags.len() < required_prefix.len()
        || folder.tags[..required_prefix.len()] != required_prefix
    {
        return Err(format!(
            "folderChange required property prefix was not in documented order: {:x?}",
            folder.tags
        ));
    }
    let display_name_position = folder
        .tags
        .iter()
        .position(|tag| *tag == PID_TAG_DISPLAY_NAME_W)
        .ok_or_else(|| {
            format!(
                "folderChange missing PidTagDisplayName after identity prefix: {:x?}",
                folder.tags
            )
        })?;
    for tag in &folder.tags[required_prefix.len()..display_name_position] {
        if *tag != PID_TAG_ENTRY_ID {
            return Err(format!(
                "folderChange unexpected property before PidTagDisplayName: 0x{tag:08x}"
            ));
        }
    }
    if let Some(container_class_position) = folder
        .tags
        .iter()
        .position(|tag| *tag == PID_TAG_CONTAINER_CLASS_W)
    {
        if let Some(folder_id_position) =
            folder.tags.iter().position(|tag| *tag == PID_TAG_FOLDER_ID)
        {
            if container_class_position < folder_id_position {
                return Err("PidTagContainerClass appeared before PidTagFolderId".into());
            }
        }
        if let Some(parent_folder_id_position) = folder
            .tags
            .iter()
            .position(|tag| *tag == PID_TAG_PARENT_FOLDER_ID)
        {
            if container_class_position < parent_folder_id_position {
                return Err("PidTagContainerClass appeared before PidTagParentFolderId".into());
            }
        }
    }
    let source_key = folder
        .source_key
        .ok_or("folderChange missing PidTagSourceKey")?;
    let parent_source_key = folder
        .parent_source_key
        .ok_or("folderChange missing PidTagParentSourceKey")?;
    let change_key = folder
        .change_key
        .ok_or("folderChange missing PidTagChangeKey")?;
    let display_name = folder
        .display_name
        .ok_or("folderChange missing PidTagDisplayName")?;
    strict_validate_source_or_change_key(&source_key)?;
    strict_validate_source_or_change_key(&change_key)?;
    if !parent_source_key.is_empty() {
        strict_validate_source_or_change_key(&parent_source_key)?;
        if !seen_source_keys
            .iter()
            .any(|source_key| source_key.as_slice() == parent_source_key.as_slice())
        {
            return Err(format!(
                "folderChange for {display_name} appeared before its parent folder"
            ));
        }
    }
    seen_source_keys.push(source_key.clone());
    folder_changes.push(StrictHierarchyFolderChange {
        source_key,
        parent_source_key,
        change_key,
        display_name,
        container_class: folder.container_class,
        default_post_message_class: folder.default_post_message_class,
        folder_id: folder.folder_id,
        parent_folder_id: folder.parent_folder_id,
        folder_type: folder.folder_type,
        content_count: folder.content_count,
        content_unread_count: folder.content_unread_count,
        local_commit_time_max: folder.local_commit_time_max,
        deleted_count_total: folder.deleted_count_total,
    });
    Ok(())
}

fn strict_decode_utf16z(bytes: &[u8]) -> Result<String, String> {
    if bytes.len() < 2 || bytes.len() % 2 != 0 || bytes[bytes.len() - 2..] != [0, 0] {
        return Err("UTF-16 property is not null-terminated".into());
    }
    let units = bytes[..bytes.len() - 2]
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect::<Vec<_>>();
    String::from_utf16(&units).map_err(|_| "UTF-16 property contains invalid data".into())
}

fn strict_decode_string8z(bytes: &[u8]) -> Result<String, String> {
    if bytes.last() != Some(&0) {
        return Err("String8 property is not null-terminated".into());
    }
    String::from_utf8(bytes[..bytes.len().saturating_sub(1)].to_vec())
        .map_err(|_| "String8 property contains invalid data".into())
}

fn read_rop_utf16z(bytes: &[u8], offset: &mut usize) -> Result<String, String> {
    let start = *offset;
    while *offset + 1 < bytes.len() {
        if bytes[*offset] == 0 && bytes[*offset + 1] == 0 {
            *offset += 2;
            return strict_decode_utf16z(&bytes[start..*offset]);
        }
        *offset += 2;
    }
    Err("ROP row UTF-16 property is not null-terminated".into())
}

fn read_rop_ascii_z(bytes: &[u8], offset: &mut usize) -> Result<String, String> {
    let start = *offset;
    while *offset < bytes.len() {
        if bytes[*offset] == 0 {
            let value = String::from_utf8(bytes[start..*offset].to_vec())
                .map_err(|_| "ROP row String8 property contains invalid UTF-8".to_string())?;
            *offset += 1;
            return Ok(value);
        }
        *offset += 1;
    }
    Err("ROP row String8 property is not null-terminated".into())
}

fn read_rop_binary_u16<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<&'a [u8], String> {
    let len = u16::from_le_bytes(
        bytes
            .get(*offset..offset.saturating_add(2))
            .ok_or("ROP row binary property is missing its length")?
            .try_into()
            .unwrap(),
    ) as usize;
    *offset += 2;
    let value = bytes
        .get(*offset..offset.saturating_add(len))
        .ok_or("ROP row binary property overruns row data")?;
    *offset += len;
    Ok(value)
}

fn hierarchy_query_display_container_rows(
    response_rops: &[u8],
    query_offset: usize,
) -> Result<Vec<(String, String)>, String> {
    if response_rops.get(query_offset) != Some(&0x15) {
        return Err("missing RopQueryRows response".into());
    }
    let row_count = u16::from_le_bytes(
        response_rops[query_offset + 7..query_offset + 9]
            .try_into()
            .unwrap(),
    ) as usize;
    let mut offset = query_offset + 9;
    let mut rows = Vec::new();
    for _ in 0..row_count {
        if response_rops.get(offset) != Some(&0) {
            return Err("standard property row did not start with success status".into());
        }
        offset += 1;
        let display_name = read_rop_utf16z(response_rops, &mut offset)?;
        let container_class = read_rop_utf16z(response_rops, &mut offset)?;
        rows.push((display_name, container_class));
    }
    Ok(rows)
}

#[derive(Debug)]
struct HierarchyCalendarFolderRow {
    display_name: String,
    entry_id: Vec<u8>,
    instance_key: Vec<u8>,
    source_key: Vec<u8>,
    container_class: String,
    default_post_message_class_a: String,
    default_post_message_class_w: String,
}

fn hierarchy_query_calendar_contract_rows(
    response_rops: &[u8],
    query_offset: usize,
) -> Result<Vec<HierarchyCalendarFolderRow>, String> {
    if response_rops.get(query_offset) != Some(&0x15) {
        return Err("missing RopQueryRows response".into());
    }
    let row_count = u16::from_le_bytes(
        response_rops[query_offset + 7..query_offset + 9]
            .try_into()
            .unwrap(),
    ) as usize;
    let mut offset = query_offset + 9;
    let mut rows = Vec::new();
    for _ in 0..row_count {
        if response_rops.get(offset) != Some(&0) {
            return Err("standard property row did not start with success status".into());
        }
        offset += 1;
        let display_name = read_rop_utf16z(response_rops, &mut offset)?;
        let entry_id = read_rop_binary_u16(response_rops, &mut offset)?.to_vec();
        let instance_key = read_rop_binary_u16(response_rops, &mut offset)?.to_vec();
        let source_key = read_rop_binary_u16(response_rops, &mut offset)?.to_vec();
        let container_class = read_rop_utf16z(response_rops, &mut offset)?;
        let default_post_message_class_a = read_rop_ascii_z(response_rops, &mut offset)?;
        let default_post_message_class_w = read_rop_utf16z(response_rops, &mut offset)?;
        rows.push(HierarchyCalendarFolderRow {
            display_name,
            entry_id,
            instance_key,
            source_key,
            container_class,
            default_post_message_class_a,
            default_post_message_class_w,
        });
    }
    Ok(rows)
}

fn strict_validate_source_or_change_key(value: &[u8]) -> Result<(), String> {
    if value.len() != 22 || !value.starts_with(&mapi_mailstore::STORE_REPLICA_GUID) {
        return Err("source/change key is not a 22-byte REPLGUID-scoped XID".into());
    }
    if value[16..22] == [0; 6] {
        return Err("source/change key contains a zero GLOBCNT".into());
    }
    Ok(())
}

fn strict_validate_replguid_globset(value: &[u8]) -> Result<(), String> {
    let _ = strict_replguid_globset_ranges(value)?;
    Ok(())
}

fn strict_replguid_globset_contains_counter(value: &[u8], counter: &[u8]) -> Result<bool, String> {
    let counter = strict_globcnt_to_u64(counter)?;
    Ok(strict_replguid_globset_ranges(value)?
        .into_iter()
        .any(|(low, high)| low <= counter && counter <= high))
}

fn strict_replguid_globset_ranges(value: &[u8]) -> Result<Vec<(u64, u64)>, String> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    if value.len() < 17 || !value.starts_with(&mapi_mailstore::STORE_REPLICA_GUID) {
        return Err("REPLGUID-based IDSET/CNSET is missing the store replica GUID".into());
    }
    let mut ranges = Vec::new();
    let mut offset = 16;
    loop {
        let command = *value
            .get(offset)
            .ok_or("REPLGUID-based IDSET/CNSET missing end command")?;
        offset += 1;
        match command {
            0x00 => {
                if offset != value.len() {
                    return Err("trailing bytes after GLOBSET end command".into());
                }
                return Ok(ranges);
            }
            0x52 => {
                let low = strict_globcnt_to_u64(read_strict_slice(value, offset, 6)?)?;
                offset += 6;
                let high = strict_globcnt_to_u64(read_strict_slice(value, offset, 6)?)?;
                offset += 6;
                if low == 0 || high < low {
                    return Err("invalid GLOBSET range".into());
                }
                ranges.push((low, high));
            }
            _ => return Err(format!("unsupported GLOBSET command 0x{command:02x}")),
        }
    }
}

fn strict_globcnt_to_u64(bytes: &[u8]) -> Result<u64, String> {
    if bytes.len() != 6 {
        return Err("GLOBCNT must be six bytes".into());
    }
    Ok(bytes
        .iter()
        .fold(0u64, |value, byte| (value << 8) | u64::from(*byte)))
}

fn read_strict_u32(bytes: &[u8], offset: usize) -> Result<u32, String> {
    let slice = read_strict_slice(bytes, offset, 4)?;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_strict_u16(bytes: &[u8], offset: usize) -> Result<u16, String> {
    let slice = read_strict_slice(bytes, offset, 2)?;
    Ok(u16::from_le_bytes(slice.try_into().unwrap()))
}

fn read_strict_slice(bytes: &[u8], offset: usize, len: usize) -> Result<&[u8], String> {
    bytes
        .get(offset..offset.saturating_add(len))
        .ok_or_else(|| format!("FastTransfer atom at offset {offset} overruns stream"))
}

fn strict_test_xid(counter: u64) -> Vec<u8> {
    let mut value = mapi_mailstore::STORE_REPLICA_GUID.to_vec();
    value.extend_from_slice(&globcnt_bytes(counter));
    value
}

fn strict_test_replguid_globset(counters: &[u64]) -> Vec<u8> {
    let mut value = mapi_mailstore::STORE_REPLICA_GUID.to_vec();
    for counter in counters {
        value.push(0x52);
        value.extend_from_slice(&globcnt_bytes(*counter));
        value.extend_from_slice(&globcnt_bytes(*counter));
    }
    value.push(0);
    value
}

fn strict_test_replid_globset(counters: &[u64]) -> Vec<u8> {
    let mut value = 1u16.to_le_bytes().to_vec();
    for counter in counters {
        value.push(0x52);
        value.extend_from_slice(&globcnt_bytes(*counter));
        value.extend_from_slice(&globcnt_bytes(*counter));
    }
    value.push(0);
    value
}

fn mapi_binary_property(tag: u32, value: &[u8]) -> Vec<u8> {
    let mut property = tag.to_le_bytes().to_vec();
    property.extend_from_slice(&(value.len() as u32).to_le_bytes());
    property.extend_from_slice(value);
    property
}

fn mapi_message_global_counter(id: &Uuid) -> u64 {
    crate::mapi::identity::mapped_mapi_object_id(id).unwrap_or_else(|| test_mapi_uuid_id(id)) >> 16
}

fn mapi_message_cnset_property(tag: u32, changes: &[u64]) -> Vec<u8> {
    mapi_binary_property(tag, &strict_test_replguid_globset(changes))
}

fn mapi_deleted_message_idset_property(ids: &[Uuid]) -> Vec<u8> {
    let counters = ids
        .iter()
        .map(mapi_message_global_counter)
        .collect::<Vec<_>>();
    mapi_binary_property(
        META_TAG_IDSET_DELETED,
        &strict_test_replid_globset(&counters),
    )
}

fn mapi_read_message_idset_property(ids: &[Uuid]) -> Vec<u8> {
    let counters = ids
        .iter()
        .map(mapi_message_global_counter)
        .collect::<Vec<_>>();
    mapi_binary_property(META_TAG_IDSET_READ, &strict_test_replid_globset(&counters))
}

fn mapi_unread_message_idset_property(ids: &[Uuid]) -> Vec<u8> {
    let counters = ids
        .iter()
        .map(mapi_message_global_counter)
        .collect::<Vec<_>>();
    mapi_binary_property(
        META_TAG_IDSET_UNREAD,
        &strict_test_replid_globset(&counters),
    )
}

#[derive(Debug)]
struct StrictContentSyncStream {
    message_changes: Vec<StrictContentMessageChange>,
    deleted_idset: Option<Vec<u8>>,
    read_idset: Option<Vec<u8>>,
    unread_idset: Option<Vec<u8>>,
    idset_given: Vec<u8>,
    cnset_seen: Vec<u8>,
    cnset_seen_fai: Vec<u8>,
    cnset_read: Vec<u8>,
}

#[derive(Debug)]
struct StrictContentMessageChange {
    source_key: Vec<u8>,
    parent_source_key: Vec<u8>,
    change_key: Vec<u8>,
    predecessor_change_list: Vec<u8>,
    entry_id: Vec<u8>,
    body_tags: Vec<u32>,
    mid: Option<u64>,
    change_number: Option<u64>,
    associated: bool,
    subject: String,
}

#[derive(Default)]
struct StrictContentMessageBuilder {
    header_tags: Vec<u32>,
    body_tags: Vec<u32>,
    source_key: Option<Vec<u8>>,
    parent_source_key: Option<Vec<u8>>,
    change_key: Option<Vec<u8>>,
    predecessor_change_list: Option<Vec<u8>>,
    entry_id: Option<Vec<u8>>,
    mid: Option<u64>,
    change_number: Option<u64>,
    associated: Option<bool>,
    subject: Option<String>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum StrictContentSection {
    None,
    Progress,
    MessageHeader,
    MessageBody,
    Recipient,
    Attachment,
    EmbeddedMessage,
    Deletions,
    ReadState,
    State,
}

fn strict_content_sync_transfer_from_response(
    response_rops: &[u8],
) -> Result<StrictContentSyncStream, String> {
    let chunks = mapi_fast_transfer_chunks(response_rops);
    if chunks.len() != 1 {
        return Err(format!(
            "expected one completed FastTransfer chunk, got {}",
            chunks.len()
        ));
    }
    if chunks[0].0 != 0x0003 {
        return Err(format!(
            "expected completed FastTransfer status 0x0003, got 0x{:04x}",
            chunks[0].0
        ));
    }
    strict_decode_content_sync_stream(&chunks[0].1)
}

fn strict_decode_content_sync_stream(bytes: &[u8]) -> Result<StrictContentSyncStream, String> {
    let mut offset = 0;
    let mut section = StrictContentSection::None;
    let mut current_message: Option<StrictContentMessageBuilder> = None;
    let mut message_changes = Vec::new();
    let mut deleted_idset = None;
    let mut read_idset = None;
    let mut unread_idset = None;
    let mut idset_given = None;
    let mut cnset_seen = None;
    let mut cnset_seen_fai = None;
    let mut cnset_read = None;
    let mut state_closed = false;

    while offset < bytes.len() {
        let tag = read_strict_u32(bytes, offset)?;
        if strict_content_marker(tag) {
            match tag {
                FX_INCR_SYNC_CHG => {
                    if state_closed {
                        return Err("messageChange appears after final ICS state".into());
                    }
                    if let Some(message) = current_message.take() {
                        strict_finish_content_message(message, &mut message_changes)?;
                    }
                    current_message = Some(StrictContentMessageBuilder::default());
                    section = StrictContentSection::MessageHeader;
                }
                FX_INCR_SYNC_PROGRESS_MODE | FX_INCR_SYNC_PROGRESS_PER_MSG => {
                    if state_closed {
                        return Err("progress marker appears after final ICS state".into());
                    }
                    if let Some(message) = current_message.take() {
                        strict_finish_content_message(message, &mut message_changes)?;
                    }
                    section = StrictContentSection::Progress;
                }
                FX_INCR_SYNC_MESSAGE => {
                    if section != StrictContentSection::MessageHeader {
                        return Err("IncrSyncMessage without an open messageChange header".into());
                    }
                    section = StrictContentSection::MessageBody;
                }
                FX_START_RECIP => {
                    if section != StrictContentSection::MessageBody {
                        return Err("StartRecip outside message content".into());
                    }
                    section = StrictContentSection::Recipient;
                }
                FX_END_TO_RECIP => {
                    if section != StrictContentSection::Recipient {
                        return Err("EndToRecip without StartRecip".into());
                    }
                    section = StrictContentSection::MessageBody;
                }
                FX_NEW_ATTACH => {
                    if section != StrictContentSection::MessageBody {
                        return Err("NewAttach outside message content".into());
                    }
                    section = StrictContentSection::Attachment;
                }
                FX_END_ATTACH => {
                    if section != StrictContentSection::Attachment {
                        return Err("EndAttach without NewAttach".into());
                    }
                    section = StrictContentSection::MessageBody;
                }
                FX_START_EMBED => {
                    if section != StrictContentSection::Attachment {
                        return Err("StartEmbed outside attachment content".into());
                    }
                    section = StrictContentSection::EmbeddedMessage;
                }
                FX_END_EMBED => {
                    if section != StrictContentSection::EmbeddedMessage {
                        return Err("EndEmbed without StartEmbed".into());
                    }
                    section = StrictContentSection::Attachment;
                }
                FX_INCR_SYNC_DEL => {
                    if let Some(message) = current_message.take() {
                        strict_finish_content_message(message, &mut message_changes)?;
                    }
                    section = StrictContentSection::Deletions;
                }
                FX_INCR_SYNC_READ => {
                    if let Some(message) = current_message.take() {
                        strict_finish_content_message(message, &mut message_changes)?;
                    }
                    section = StrictContentSection::ReadState;
                }
                FX_INCR_SYNC_STATE_BEGIN => {
                    if let Some(message) = current_message.take() {
                        strict_finish_content_message(message, &mut message_changes)?;
                    }
                    if state_closed {
                        return Err("duplicate final ICS state boundary".into());
                    }
                    section = StrictContentSection::State;
                }
                FX_INCR_SYNC_STATE_END => {
                    if section != StrictContentSection::State {
                        return Err("IncrSyncStateEnd without IncrSyncStateBegin".into());
                    }
                    state_closed = true;
                    section = StrictContentSection::None;
                }
                FX_INCR_SYNC_END => {
                    if !state_closed {
                        return Err("IncrSyncEnd appears before final ICS state is complete".into());
                    }
                    offset += 4;
                    if offset != bytes.len() {
                        return Err("trailing bytes after IncrSyncEnd".into());
                    }
                    break;
                }
                _ => unreachable!(),
            }
            offset += 4;
            continue;
        }

        if section == StrictContentSection::MessageBody
            && current_message
                .as_ref()
                .and_then(|message| message.subject.as_ref())
                .is_some()
            && !strict_supported_property_type(tag)
        {
            offset = strict_skip_content_message_tail(bytes, offset)?;
            continue;
        }

        let property = strict_parse_fast_transfer_property(bytes, offset)?;
        offset = property.next_offset;
        match section {
            StrictContentSection::MessageHeader => {
                let message = current_message
                    .as_mut()
                    .ok_or("message header property without current message")?;
                strict_record_content_header_property(message, property)?;
            }
            StrictContentSection::MessageBody => {
                let message = current_message
                    .as_mut()
                    .ok_or("message body property without current message")?;
                strict_record_content_body_property(message, property)?;
            }
            StrictContentSection::Progress => match property.tag {
                0x0000_0102 => {
                    if property.value.len() != 32 {
                        return Err("invalid ProgressInformation length".into());
                    }
                }
                0x0000_0003 | 0x0000_000B => {}
                tag => return Err(format!("unexpected progress property 0x{tag:08x}")),
            },
            StrictContentSection::Recipient => match property.tag {
                PID_TAG_RECIPIENT_TYPE => {
                    let recipient_type = strict_decode_i32_property(&property)?;
                    if recipient_type != 1 && recipient_type != 2 {
                        return Err(format!("unexpected recipient type {recipient_type}"));
                    }
                }
                PID_TAG_DISPLAY_NAME_W | PID_TAG_EMAIL_ADDRESS_W => {
                    let _ = strict_decode_utf16z(&property.value)?;
                }
                tag => return Err(format!("unexpected recipient property 0x{tag:08x}")),
            },
            StrictContentSection::Attachment => match property.tag {
                PID_TAG_ATTACH_NUM
                | PID_TAG_RENDERING_POSITION
                | PID_TAG_ATTACH_SIZE
                | PID_TAG_ATTACH_METHOD
                | PID_TAG_ATTACH_FLAGS => {
                    let _ = strict_decode_i32_property(&property)?;
                }
                PID_TAG_ATTACH_ENCODING | PID_TAG_ATTACH_RENDERING => {}
                PID_TAG_ATTACHMENT_HIDDEN => {
                    if property.value.len() != 2 {
                        return Err(
                            "PidTagAttachmentHidden was not encoded as a two-byte PtypBoolean"
                                .into(),
                        );
                    }
                }
                PID_TAG_ATTACH_FILENAME_W
                | PID_TAG_ATTACH_LONG_FILENAME_W
                | PID_TAG_ATTACH_MIME_TAG_W => {
                    let _ = strict_decode_utf16z(&property.value)?;
                }
                tag => return Err(format!("unexpected attachment property 0x{tag:08x}")),
            },
            StrictContentSection::EmbeddedMessage => match property.tag {
                PID_TAG_MESSAGE_CLASS_W | PID_TAG_SUBJECT_W | PID_TAG_BODY_W => {
                    let _ = strict_decode_utf16z(&property.value)?;
                }
                tag => return Err(format!("unexpected embedded message property 0x{tag:08x}")),
            },
            StrictContentSection::Deletions => match property.tag {
                META_TAG_IDSET_DELETED => {
                    if deleted_idset.replace(property.value).is_some() {
                        return Err("duplicate MetaTagIdsetDeleted in deletions".into());
                    }
                }
                tag => return Err(format!("unexpected deletion property 0x{tag:08x}")),
            },
            StrictContentSection::ReadState => match property.tag {
                META_TAG_IDSET_READ => {
                    if read_idset.replace(property.value).is_some() {
                        return Err("duplicate MetaTagIdsetRead in read-state changes".into());
                    }
                }
                META_TAG_IDSET_UNREAD => {
                    if unread_idset.replace(property.value).is_some() {
                        return Err("duplicate MetaTagIdsetUnread in read-state changes".into());
                    }
                }
                tag => return Err(format!("unexpected read-state property 0x{tag:08x}")),
            },
            StrictContentSection::State => match property.tag {
                META_TAG_IDSET_GIVEN | META_TAG_IDSET_GIVEN_BINARY => {
                    if idset_given.replace(property.value).is_some() {
                        return Err("duplicate MetaTagIdsetGiven in final ICS state".into());
                    }
                }
                META_TAG_CNSET_SEEN => {
                    if cnset_seen.replace(property.value).is_some() {
                        return Err("duplicate MetaTagCnsetSeen in final ICS state".into());
                    }
                }
                META_TAG_CNSET_SEEN_FAI => {
                    if cnset_seen_fai.replace(property.value).is_some() {
                        return Err("duplicate MetaTagCnsetSeenFAI in final ICS state".into());
                    }
                }
                META_TAG_CNSET_READ => {
                    if cnset_read.replace(property.value).is_some() {
                        return Err("duplicate MetaTagCnsetRead in final ICS state".into());
                    }
                }
                tag => return Err(format!("unexpected content state property 0x{tag:08x}")),
            },
            StrictContentSection::None => {
                return Err(format!(
                    "property 0x{:08x} appears outside content section",
                    property.tag
                ));
            }
        }
    }

    let idset_given = idset_given.ok_or("missing content MetaTagIdsetGiven")?;
    let cnset_seen = cnset_seen.ok_or("missing content MetaTagCnsetSeen")?;
    let cnset_seen_fai = cnset_seen_fai.ok_or("missing content MetaTagCnsetSeenFAI")?;
    let cnset_read = cnset_read.ok_or("missing content MetaTagCnsetRead")?;
    strict_validate_replguid_globset(&idset_given)?;
    strict_validate_replguid_globset(&cnset_seen)?;
    strict_validate_replguid_globset(&cnset_seen_fai)?;
    strict_validate_replguid_globset(&cnset_read)?;
    if let Some(value) = deleted_idset.as_deref() {
        strict_validate_replid_globset(value)?;
    }
    if let Some(value) = read_idset.as_deref() {
        strict_validate_replid_globset(value)?;
    }
    if let Some(value) = unread_idset.as_deref() {
        strict_validate_replid_globset(value)?;
    }
    for message in &message_changes {
        strict_validate_source_or_change_key(&message.source_key)?;
        strict_validate_source_or_change_key(&message.parent_source_key)?;
        strict_validate_source_or_change_key(&message.change_key)?;
        if !strict_replguid_globset_contains_counter(&idset_given, &message.source_key[16..22])? {
            return Err(format!(
                "final MetaTagIdsetGiven does not include message {}",
                message.subject
            ));
        }
        let cnset = if message.associated {
            &cnset_seen_fai
        } else {
            &cnset_seen
        };
        if !strict_replguid_globset_contains_counter(cnset, &message.change_key[16..22])? {
            let cnset_name = if message.associated {
                "MetaTagCnsetSeenFAI"
            } else {
                "MetaTagCnsetSeen"
            };
            return Err(format!(
                "final {cnset_name} does not include message {} change key",
                message.subject
            ));
        }
    }

    Ok(StrictContentSyncStream {
        message_changes,
        deleted_idset,
        read_idset,
        unread_idset,
        idset_given,
        cnset_seen,
        cnset_seen_fai,
        cnset_read,
    })
}

fn strict_content_marker(tag: u32) -> bool {
    matches!(
        tag,
        FX_INCR_SYNC_CHG
            | FX_INCR_SYNC_MESSAGE
            | FX_INCR_SYNC_PROGRESS_MODE
            | FX_INCR_SYNC_PROGRESS_PER_MSG
            | FX_NEW_ATTACH
            | FX_START_EMBED
            | FX_END_EMBED
            | FX_START_RECIP
            | FX_END_TO_RECIP
            | FX_END_ATTACH
            | FX_INCR_SYNC_DEL
            | FX_INCR_SYNC_READ
            | FX_INCR_SYNC_STATE_BEGIN
            | FX_INCR_SYNC_STATE_END
            | FX_INCR_SYNC_END
    )
}

fn strict_supported_property_type(tag: u32) -> bool {
    matches!(
        tag & 0x0000_FFFF,
        0x0002 | 0x0003 | 0x000B | 0x0014 | 0x001E | 0x001F | 0x0040 | 0x0048 | 0x0102 | 0x101F
    )
}

fn strict_record_content_header_property(
    message: &mut StrictContentMessageBuilder,
    property: StrictFastTransferProperty,
) -> Result<(), String> {
    if message.header_tags.contains(&property.tag) {
        return Err(format!(
            "duplicate property 0x{:08x} inside messageChangeHeader",
            property.tag
        ));
    }
    message.header_tags.push(property.tag);
    match property.tag {
        PID_TAG_PARENT_SOURCE_KEY => message.parent_source_key = Some(property.value),
        PID_TAG_SOURCE_KEY => message.source_key = Some(property.value),
        PID_TAG_CHANGE_KEY => message.change_key = Some(property.value),
        PID_TAG_PREDECESSOR_CHANGE_LIST => message.predecessor_change_list = Some(property.value),
        PID_TAG_MID => message.mid = Some(strict_decode_u64_property(&property)?),
        PID_TAG_CHANGE_NUMBER => {
            message.change_number = Some(strict_decode_change_number_property(&property)?)
        }
        PID_TAG_MESSAGE_SIZE => {
            let _ = strict_decode_i32_property(&property)?;
        }
        PID_TAG_ASSOCIATED => {
            if property.value.len() != 2 {
                return Err("PidTagAssociated was not encoded as a two-byte PtypBoolean".into());
            }
            message.associated = Some(u16::from_le_bytes(property.value.try_into().unwrap()) != 0);
        }
        PID_TAG_LAST_MODIFICATION_TIME => {
            let _ = strict_decode_u64_property(&property)?;
        }
        tag => {
            return Err(format!(
                "unexpected messageChangeHeader property 0x{tag:08x}"
            ))
        }
    }
    Ok(())
}

fn strict_record_content_body_property(
    message: &mut StrictContentMessageBuilder,
    property: StrictFastTransferProperty,
) -> Result<(), String> {
    if message.body_tags.contains(&property.tag) {
        return Err(format!(
            "duplicate property 0x{:08x} inside message content",
            property.tag
        ));
    }
    message.body_tags.push(property.tag);
    match property.tag {
        PID_TAG_PARENT_SOURCE_KEY => message.parent_source_key = Some(property.value),
        PID_TAG_ENTRY_ID => message.entry_id = Some(property.value),
        PID_TAG_SUBJECT_W => message.subject = Some(strict_decode_utf16z(&property.value)?),
        PID_TAG_NORMALIZED_SUBJECT_A => {
            message.subject = Some(strict_decode_string8z(&property.value)?)
        }
        PID_TAG_MESSAGE_FLAGS | PID_TAG_FLAG_STATUS => {
            let _ = strict_decode_u32_property(&property)?;
        }
        PID_TAG_MESSAGE_SIZE => {
            let _ = strict_decode_i32_property(&property)?;
        }
        _ => {}
    }
    Ok(())
}

fn strict_finish_content_message(
    message: StrictContentMessageBuilder,
    message_changes: &mut Vec<StrictContentMessageChange>,
) -> Result<(), String> {
    let required_prefix = [
        PID_TAG_SOURCE_KEY,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_ASSOCIATED,
    ];
    if message.header_tags.len() < required_prefix.len()
        || message.header_tags[..required_prefix.len()] != required_prefix
    {
        return Err(format!(
            "messageChangeHeader required property prefix was not in documented order: {:x?}",
            message.header_tags
        ));
    }
    let source_key = message
        .source_key
        .ok_or("messageChange missing PidTagSourceKey")?;
    let parent_source_key = message
        .parent_source_key
        .ok_or("message content missing PidTagParentSourceKey")?;
    let change_key = message
        .change_key
        .ok_or("messageChange missing PidTagChangeKey")?;
    let predecessor_change_list = message
        .predecessor_change_list
        .ok_or("messageChange missing PidTagPredecessorChangeList")?;
    let associated = message
        .associated
        .ok_or("messageChange missing PidTagAssociated")?;
    let subject = message.subject.unwrap_or_default();
    message_changes.push(StrictContentMessageChange {
        source_key,
        parent_source_key,
        change_key,
        predecessor_change_list,
        entry_id: message.entry_id.unwrap_or_default(),
        body_tags: message.body_tags,
        mid: message.mid,
        change_number: message.change_number,
        associated,
        subject,
    });
    Ok(())
}

fn strict_skip_content_message_tail(bytes: &[u8], offset: usize) -> Result<usize, String> {
    let mut offset = offset;
    let recipient_count = read_strict_u16(bytes, offset)? as usize;
    offset += 2;
    for _ in 0..recipient_count {
        let _recipient_type = *read_strict_slice(bytes, offset, 1)?
            .first()
            .ok_or("recipient type missing")?;
        offset += 1;
        offset = strict_skip_prefixed_bytes(bytes, offset)?;
        offset = strict_skip_prefixed_bytes(bytes, offset)?;
    }
    let attachment_count = read_strict_u16(bytes, offset)? as usize;
    offset += 2;
    for _ in 0..attachment_count {
        offset = strict_skip_prefixed_bytes(bytes, offset)?;
        offset = strict_skip_prefixed_bytes(bytes, offset)?;
        let _size = read_strict_slice(bytes, offset, 8)?;
        offset += 8;
        offset = strict_skip_prefixed_bytes(bytes, offset)?;
    }
    Ok(offset)
}

fn strict_skip_prefixed_bytes(bytes: &[u8], offset: usize) -> Result<usize, String> {
    let len = read_strict_u16(bytes, offset)? as usize;
    let start = offset + 2;
    let end = start.saturating_add(len);
    let _ = read_strict_slice(bytes, start, len)?;
    Ok(end)
}

fn strict_validate_replid_globset(value: &[u8]) -> Result<(), String> {
    let _ = strict_replid_globset_ranges(value)?;
    Ok(())
}

fn strict_replid_globset_contains_counter(value: &[u8], counter: &[u8]) -> Result<bool, String> {
    let counter = strict_globcnt_to_u64(counter)?;
    Ok(strict_replid_globset_ranges(value)?
        .into_iter()
        .any(|(low, high)| low <= counter && counter <= high))
}

fn strict_replid_globset_ranges(value: &[u8]) -> Result<Vec<(u64, u64)>, String> {
    let replid = value
        .get(0..2)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .unwrap_or(0);
    if value.len() < 3 || replid == 0 {
        return Err("REPLID-based IDSET is missing the store REPLID".into());
    }
    let mut ranges = Vec::new();
    let mut stack = Vec::new();
    let mut push_lengths = Vec::new();
    let mut offset = 2;
    loop {
        let command = *value
            .get(offset)
            .ok_or("REPLID-based IDSET missing end command")?;
        offset += 1;
        match command {
            0x00 => {
                if offset != value.len() {
                    return Err("trailing bytes after REPLID GLOBSET end command".into());
                }
                if !stack.is_empty() {
                    return Err("non-empty REPLID GLOBSET stack at end command".into());
                }
                return Ok(ranges);
            }
            1..=6 => {
                let push_len = usize::from(command);
                let bytes = read_strict_slice(value, offset, push_len)?;
                offset += push_len;
                if stack.len() + push_len > 6 {
                    return Err("REPLID GLOBSET push overflows GLOBCNT".into());
                }
                stack.extend_from_slice(bytes);
                if stack.len() == 6 {
                    let counter = strict_globcnt_to_u64(&stack)?;
                    ranges.push((counter, counter));
                    stack.truncate(stack.len() - push_len);
                } else {
                    push_lengths.push(push_len);
                }
            }
            0x42 => {
                if stack.len() != 5 {
                    return Err("REPLID GLOBSET bitmask requires five-byte stack".into());
                }
                let start = *read_strict_slice(value, offset, 1)?.first().unwrap();
                offset += 1;
                let mask = *read_strict_slice(value, offset, 1)?.first().unwrap();
                offset += 1;
                let mut values = vec![start];
                for bit in 0..8 {
                    if mask & (1 << bit) != 0 {
                        values.push(start.saturating_add(1 + bit));
                    }
                }
                for (low, high) in coalesced_u8_ranges(values) {
                    let mut low_bytes = stack.clone();
                    low_bytes.push(low);
                    let mut high_bytes = stack.clone();
                    high_bytes.push(high);
                    ranges.push((
                        strict_globcnt_to_u64(&low_bytes)?,
                        strict_globcnt_to_u64(&high_bytes)?,
                    ));
                }
            }
            0x50 => {
                let Some(pop_len) = push_lengths.pop() else {
                    return Err("REPLID GLOBSET pop without push".into());
                };
                if pop_len > stack.len() {
                    return Err("REPLID GLOBSET pop underflows stack".into());
                }
                stack.truncate(stack.len() - pop_len);
            }
            0x52 => {
                let suffix_len = 6usize.saturating_sub(stack.len());
                let low_suffix = read_strict_slice(value, offset, suffix_len)?;
                offset += suffix_len;
                let high_suffix = read_strict_slice(value, offset, suffix_len)?;
                offset += suffix_len;
                let mut low_bytes = stack.clone();
                low_bytes.extend_from_slice(low_suffix);
                let mut high_bytes = stack.clone();
                high_bytes.extend_from_slice(high_suffix);
                let low = strict_globcnt_to_u64(&low_bytes)?;
                let high = strict_globcnt_to_u64(&high_bytes)?;
                if low == 0 || high < low {
                    return Err("invalid REPLID GLOBSET range".into());
                }
                ranges.push((low, high));
            }
            _ => {
                return Err(format!(
                    "unsupported REPLID GLOBSET command 0x{command:02x}"
                ))
            }
        }
    }
}

fn coalesced_u8_ranges(mut values: Vec<u8>) -> Vec<(u8, u8)> {
    if values.is_empty() {
        return Vec::new();
    }
    values.sort_unstable();
    values.dedup();
    let mut ranges = Vec::new();
    let mut low = values[0];
    let mut high = values[0];
    for value in values.into_iter().skip(1) {
        if value == high.saturating_add(1) {
            high = value;
        } else {
            ranges.push((low, high));
            low = value;
            high = value;
        }
    }
    ranges.push((low, high));
    ranges
}

#[test]
fn microsoft_oxcfxics_idset_serialization_example_decodes_replid_globsets() {
    let example = [
        0x01, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x52, 0x05, 0x06, 0x01, 0x10, 0x50, 0x00,
        0x02, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00,
    ];

    assert_eq!(
        strict_replid_globset_ranges(&example[..15]).unwrap(),
        vec![(5, 6), (16, 16)]
    );
    assert_eq!(
        strict_replid_globset_ranges(&example[15..]).unwrap(),
        vec![(9, 9)]
    );
}

fn strict_push_binary_property(bytes: &mut Vec<u8>, tag: u32, value: &[u8]) {
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
    bytes.extend_from_slice(value);
}

fn strict_push_i32_property(bytes: &mut Vec<u8>, tag: u32, value: i32) {
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn strict_push_i64_property(bytes: &mut Vec<u8>, tag: u32, value: i64) {
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn strict_push_utf16_property(bytes: &mut Vec<u8>, tag: u32, value: &str) {
    bytes.extend_from_slice(&tag.to_le_bytes());
    let value = utf16z(value);
    bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&value);
}

fn strict_push_folder_change(
    bytes: &mut Vec<u8>,
    parent_source_key: &[u8],
    source_counter: u64,
    change_counter: u64,
    name: &str,
    boolean_width: usize,
) {
    bytes.extend_from_slice(&FX_INCR_SYNC_CHG.to_le_bytes());
    strict_push_binary_property(bytes, PID_TAG_PARENT_SOURCE_KEY, parent_source_key);
    strict_push_binary_property(bytes, PID_TAG_SOURCE_KEY, &strict_test_xid(source_counter));
    strict_push_i64_property(bytes, PID_TAG_LAST_MODIFICATION_TIME, 1);
    strict_push_binary_property(bytes, PID_TAG_CHANGE_KEY, &strict_test_xid(change_counter));
    strict_push_binary_property(
        bytes,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &strict_test_xid(change_counter),
    );
    strict_push_utf16_property(bytes, PID_TAG_DISPLAY_NAME_W, name);
    strict_push_utf16_property(bytes, PID_TAG_CONTAINER_CLASS_W, "IPF.Note");
    strict_push_i32_property(bytes, 0x3602_0003, 0);
    bytes.extend_from_slice(&PID_TAG_SUBFOLDERS.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    if boolean_width > 2 {
        bytes.extend(std::iter::repeat_n(0, boolean_width - 2));
    }
}

fn strict_push_final_hierarchy_state(bytes: &mut Vec<u8>, source_ids: &[u64], changes: &[u64]) {
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_BEGIN.to_le_bytes());
    strict_push_binary_property(
        bytes,
        META_TAG_IDSET_GIVEN,
        &strict_test_replguid_globset(source_ids),
    );
    strict_push_binary_property(
        bytes,
        META_TAG_CNSET_SEEN,
        &strict_test_replguid_globset(changes),
    );
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_END.to_le_bytes());
    bytes.extend_from_slice(&FX_INCR_SYNC_END.to_le_bytes());
}

#[test]
fn strict_hierarchy_decoder_rejects_child_before_parent() {
    let parent_source_key = strict_test_xid(5);
    let mut bytes = Vec::new();
    strict_push_folder_change(&mut bytes, &parent_source_key, 6, 200, "Archive", 2);
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 2);
    strict_push_final_hierarchy_state(&mut bytes, &[5, 6], &[100, 200]);

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("before its parent"));
}

#[test]
fn strict_hierarchy_decoder_rejects_misaligned_boolean_lexical_size() {
    let mut bytes = Vec::new();
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 4);
    strict_push_final_hierarchy_state(&mut bytes, &[5], &[100]);

    assert!(strict_decode_hierarchy_sync_stream(&bytes).is_err());
}

#[test]
fn strict_hierarchy_decoder_rejects_missing_final_cnset() {
    let mut bytes = Vec::new();
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 2);
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_BEGIN.to_le_bytes());
    strict_push_binary_property(
        &mut bytes,
        META_TAG_IDSET_GIVEN,
        &strict_test_replguid_globset(&[5]),
    );
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_END.to_le_bytes());
    bytes.extend_from_slice(&FX_INCR_SYNC_END.to_le_bytes());

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("missing hierarchy IDSET or CNSET"));
}

#[test]
fn strict_hierarchy_decoder_rejects_folder_change_after_final_state() {
    let mut bytes = Vec::new();
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 2);
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_BEGIN.to_le_bytes());
    strict_push_binary_property(
        &mut bytes,
        META_TAG_IDSET_GIVEN,
        &strict_test_replguid_globset(&[5]),
    );
    strict_push_binary_property(
        &mut bytes,
        META_TAG_CNSET_SEEN,
        &strict_test_replguid_globset(&[100]),
    );
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_END.to_le_bytes());
    strict_push_folder_change(&mut bytes, &[], 6, 200, "Late", 2);
    bytes.extend_from_slice(&FX_INCR_SYNC_END.to_le_bytes());

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("after final ICS state"));
}

#[test]
fn strict_hierarchy_decoder_rejects_duplicate_folder_property() {
    let mut bytes = Vec::new();
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 2);
    strict_push_utf16_property(&mut bytes, PID_TAG_DISPLAY_NAME_W, "Duplicate");
    strict_push_final_hierarchy_state(&mut bytes, &[5], &[100]);

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("duplicate property"));
}

#[test]
fn strict_hierarchy_decoder_rejects_message_change_in_hierarchy_stream() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&FX_INCR_SYNC_CHG.to_le_bytes());
    strict_push_i64_property(&mut bytes, PID_TAG_MID, 123);
    strict_push_final_hierarchy_state(&mut bytes, &[], &[]);

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("message change property"));
}

#[test]
fn strict_hierarchy_decoder_rejects_final_state_missing_folder_id() {
    let mut bytes = Vec::new();
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 2);
    strict_push_final_hierarchy_state(&mut bytes, &[], &[100]);

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("does not include folder"));
}

#[test]
fn strict_hierarchy_decoder_rejects_non_replguid_final_state() {
    let mut bytes = Vec::new();
    let mut wrong_idset = vec![0xAA; 16];
    wrong_idset.push(0);
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 2);
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_BEGIN.to_le_bytes());
    strict_push_binary_property(&mut bytes, META_TAG_IDSET_GIVEN, &wrong_idset);
    strict_push_binary_property(
        &mut bytes,
        META_TAG_CNSET_SEEN,
        &strict_test_replguid_globset(&[100]),
    );
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_END.to_le_bytes());
    bytes.extend_from_slice(&FX_INCR_SYNC_END.to_le_bytes());

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("missing the store replica GUID"));
}

fn mapi_last_binary_property(bytes: &[u8], property_tag: u32) -> Option<&[u8]> {
    let tag = property_tag.to_le_bytes();
    let offset = bytes.windows(tag.len()).rposition(|window| window == tag)?;
    let length = u32::from_le_bytes(bytes.get(offset + 4..offset + 8)?.try_into().ok()?);
    bytes.get(offset + 8..offset + 8 + length as usize)
}

fn mapi_get_properties_specific_standard_row_offset(
    bytes: &[u8],
    handle_index: u8,
) -> Result<usize, String> {
    let marker = [0x07, handle_index, 0, 0, 0, 0];
    let offset = bytes
        .windows(marker.len())
        .position(|window| window == marker)
        .ok_or("missing successful RopGetPropertiesSpecific response")?;
    match bytes.get(offset + marker.len()) {
        Some(0) => Ok(offset + marker.len()),
        Some(1) => Err("RopGetPropertiesSpecific returned flagged property row".to_string()),
        Some(value) => Err(format!(
            "RopGetPropertiesSpecific returned unknown property row kind {value:#04x}"
        )),
        None => Err("RopGetPropertiesSpecific response is missing property row kind".to_string()),
    }
}

fn mapi_sync_manifest_message_state(bytes: &[u8], subject: &str) -> Option<(u32, u32)> {
    let subject = subject.as_bytes();
    let subject_start = bytes
        .windows(subject.len())
        .position(|window| window == subject)?;
    let flags_tag = 0x0E07_0003u32.to_le_bytes();
    let flag_status_tag = 0x1090_0003u32.to_le_bytes();
    let flags_start = bytes[..subject_start]
        .windows(flags_tag.len())
        .rposition(|window| window == flags_tag)?;
    let flag_status_start = bytes[..subject_start]
        .windows(flag_status_tag.len())
        .rposition(|window| window == flag_status_tag)?;
    Some((
        u32::from_le_bytes(
            bytes
                .get(flags_start + 4..flags_start + 8)?
                .try_into()
                .ok()?,
        ),
        u32::from_le_bytes(
            bytes
                .get(flag_status_start + 4..flag_status_start + 8)?
                .try_into()
                .ok()?,
        ),
    ))
}

fn mapi_fast_transfer_chunks(bytes: &[u8]) -> Vec<(u16, Vec<u8>)> {
    let mut chunks = Vec::new();
    let mut offset = 0;
    while offset + 15 <= bytes.len() {
        if bytes[offset] != 0x4E {
            offset += 1;
            continue;
        }
        let return_value = u32::from_le_bytes([
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
        ]);
        if return_value != 0 {
            offset += 1;
            continue;
        }
        let status = u16::from_le_bytes([bytes[offset + 6], bytes[offset + 7]]);
        let transfer_buffer_size =
            u16::from_le_bytes([bytes[offset + 13], bytes[offset + 14]]) as usize;
        let transfer_buffer_start = offset + 15;
        let transfer_buffer_end = transfer_buffer_start.saturating_add(transfer_buffer_size);
        let Some(chunk) = bytes.get(transfer_buffer_start..transfer_buffer_end) else {
            offset += 1;
            continue;
        };
        chunks.push((status, chunk.to_vec()));
        offset = transfer_buffer_end;
    }
    chunks
}

fn assert_mapi_fast_transfer_marker_sequence(buffer: &[u8], markers: &[u32]) {
    let mut offset = 0;
    for marker in markers {
        let marker_bytes = marker.to_le_bytes();
        let relative_offset = buffer[offset..]
            .windows(marker_bytes.len())
            .position(|window| window == marker_bytes)
            .unwrap_or_else(|| panic!("missing FastTransfer marker 0x{marker:08x}"));
        offset += relative_offset + marker_bytes.len();
    }
}

fn utf16z(value: &str) -> Vec<u8> {
    let mut bytes = value
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes
}

fn append_mapi_utf16_property(values: &mut Vec<u8>, property_tag: u32, value: &str) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(&utf16z(value));
}

fn append_mapi_string8_property(values: &mut Vec<u8>, property_tag: u32, value: &str) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(value.as_bytes());
    values.push(0);
}

fn append_mapi_binary_property(values: &mut Vec<u8>, property_tag: u32, value: &[u8]) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(&(value.len() as u16).to_le_bytes());
    values.extend_from_slice(value);
}

fn append_mapi_bool_property(values: &mut Vec<u8>, property_tag: u32, value: bool) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.push(value as u8);
}

fn append_mapi_guid_property(values: &mut Vec<u8>, property_tag: u32, value: [u8; 16]) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(&value);
}

fn append_mapi_multi_binary_property(values: &mut Vec<u8>, property_tag: u32, items: &[&[u8]]) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for item in items {
        values.extend_from_slice(&(item.len() as u16).to_le_bytes());
        values.extend_from_slice(item);
    }
}

fn append_mapi_multi_utf16_property(values: &mut Vec<u8>, property_tag: u32, items: &[&str]) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for item in items {
        values.extend_from_slice(&utf16z(item));
    }
}

fn valid_swapped_todo_data() -> Vec<u8> {
    let mut value = vec![0; 540];
    value[0..4].copy_from_slice(&1u32.to_le_bytes());
    value[4..8].copy_from_slice(&0x0000_00F9u32.to_le_bytes());
    value[8..12].copy_from_slice(&8u32.to_le_bytes());
    for (index, unit) in "Follow up".encode_utf16().enumerate() {
        let offset = 12 + index * 2;
        value[offset..offset + 2].copy_from_slice(&unit.to_le_bytes());
    }
    value[524..528].copy_from_slice(&1_000_000u32.to_le_bytes());
    value[528..532].copy_from_slice(&1_001_440u32.to_le_bytes());
    value[532..536].copy_from_slice(&1_000_030u32.to_le_bytes());
    value[536..540].copy_from_slice(&1u32.to_le_bytes());
    value
}

fn append_mapi_i32_property(values: &mut Vec<u8>, property_tag: u32, value: i32) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(&value.to_le_bytes());
}

fn append_mapi_i64_property(values: &mut Vec<u8>, property_tag: u32, value: i64) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(&value.to_le_bytes());
}

fn append_mapi_wire_id(buffer: &mut Vec<u8>, object_id: u64) {
    buffer.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(object_id).unwrap(),
    );
}

fn append_mapi_trailing_replid_wire_id(buffer: &mut Vec<u8>, global_counter: u64) {
    buffer.extend_from_slice(&globcnt_bytes(global_counter));
    buffer.extend_from_slice(&1u16.to_le_bytes());
}

fn mapi_wire_id_bytes(object_id: u64) -> [u8; 8] {
    crate::mapi::identity::wire_id_bytes_from_object_id(object_id).unwrap()
}

fn append_rop_open_folder(rops: &mut Vec<u8>, input: u8, output: u8, folder_id: u64) {
    rops.extend_from_slice(&[0x02, input, 0x00, output]);
    append_mapi_wire_id(rops, folder_id);
    rops.push(0);
}

fn append_rop_set_search_criteria(
    rops: &mut Vec<u8>,
    input: u8,
    restriction: &[u8],
    folder_ids: &[u64],
    flags: u32,
) {
    rops.extend_from_slice(&[0x30, 0x00, input]);
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(restriction);
    rops.extend_from_slice(&(folder_ids.len() as u16).to_le_bytes());
    for folder_id in folder_ids {
        append_mapi_wire_id(rops, *folder_id);
    }
    rops.extend_from_slice(&flags.to_le_bytes());
}

fn append_rop_get_search_criteria(rops: &mut Vec<u8>, input: u8) {
    rops.extend_from_slice(&[0x31, 0x00, input, 1, 1, 1]);
}

fn append_search_content(restriction: &mut Vec<u8>, property_tag: u32, value: &str) {
    restriction.push(0x03);
    restriction.extend_from_slice(&0x0001u16.to_le_bytes());
    restriction.extend_from_slice(&0x0001u16.to_le_bytes());
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    append_mapi_utf16_property(restriction, property_tag, value);
}

fn append_search_content_string8(restriction: &mut Vec<u8>, property_tag: u32, value: &str) {
    restriction.push(0x03);
    restriction.extend_from_slice(&0x0001u16.to_le_bytes());
    restriction.extend_from_slice(&0x0001u16.to_le_bytes());
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    append_mapi_string8_property(restriction, property_tag, value);
}

fn append_search_property_bool(
    restriction: &mut Vec<u8>,
    property_tag: u32,
    relop: u8,
    value: bool,
) {
    restriction.extend_from_slice(&[0x04, relop]);
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.push(value as u8);
}

fn append_search_property_u32(restriction: &mut Vec<u8>, property_tag: u32, relop: u8, value: u32) {
    restriction.extend_from_slice(&[0x04, relop]);
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.extend_from_slice(&value.to_le_bytes());
}

fn append_search_property_i64(restriction: &mut Vec<u8>, property_tag: u32, relop: u8, value: i64) {
    restriction.extend_from_slice(&[0x04, relop]);
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.extend_from_slice(&value.to_le_bytes());
}

fn append_search_bitmask(
    restriction: &mut Vec<u8>,
    property_tag: u32,
    must_be_nonzero: bool,
    mask: u32,
) {
    restriction.push(0x06);
    restriction.push(must_be_nonzero as u8);
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.extend_from_slice(&mask.to_le_bytes());
}

fn append_search_property_string(
    restriction: &mut Vec<u8>,
    property_tag: u32,
    relop: u8,
    value: &str,
) {
    restriction.extend_from_slice(&[0x04, relop]);
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    append_mapi_utf16_property(restriction, property_tag, value);
}

fn append_search_property_multi_string(
    restriction: &mut Vec<u8>,
    property_tag: u32,
    relop: u8,
    values: &[&str],
) {
    restriction.extend_from_slice(&[0x04, relop]);
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for value in values {
        restriction.extend_from_slice(&utf16z(value));
    }
}

fn append_search_property_tagged_string(
    restriction: &mut Vec<u8>,
    property_tag: u32,
    value_tag: u32,
    relop: u8,
    value: &str,
) {
    restriction.extend_from_slice(&[0x04, relop]);
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    append_mapi_utf16_property(restriction, value_tag, value);
}

fn append_search_property_binary(
    restriction: &mut Vec<u8>,
    property_tag: u32,
    relop: u8,
    value: &[u8],
) {
    restriction.extend_from_slice(&[0x04, relop]);
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    append_mapi_binary_property(restriction, property_tag, value);
}

fn append_search_exists(restriction: &mut Vec<u8>, property_tag: u32) {
    restriction.push(0x08);
    restriction.extend_from_slice(&property_tag.to_le_bytes());
}

fn append_rop_create_message(rops: &mut Vec<u8>, input: u8, output: u8, folder_id: u64) {
    rops.extend_from_slice(&[0x06, input, 0x01, output]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(rops, folder_id);
    rops.push(0);
}

fn append_rop_create_associated_message(rops: &mut Vec<u8>, input: u8, output: u8, folder_id: u64) {
    rops.extend_from_slice(&[0x06, input, 0x01, output]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(rops, folder_id);
    rops.push(1);
}

fn append_rop_set_properties(
    rops: &mut Vec<u8>,
    input: u8,
    property_count: u16,
    property_values: &[u8],
) {
    rops.extend_from_slice(&[0x0A, 0x00, input]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&property_count.to_le_bytes());
    rops.extend_from_slice(property_values);
}

fn append_rop_delete_properties(rops: &mut Vec<u8>, input: u8, property_tags: &[u32]) {
    rops.extend_from_slice(&[0x0B, 0x00, input]);
    rops.extend_from_slice(&(property_tags.len() as u16).to_le_bytes());
    for tag in property_tags {
        rops.extend_from_slice(&tag.to_le_bytes());
    }
}

fn append_rop_modify_recipients(rops: &mut Vec<u8>, input: u8, rows: &[(u32, u8, &[u8])]) {
    append_rop_modify_recipients_with_columns(
        rops,
        input,
        &[0x3001_001F, 0x3003_001F, 0x0C15_0003],
        rows,
    );
}

fn append_rop_modify_recipients_with_columns(
    rops: &mut Vec<u8>,
    input: u8,
    columns: &[u32],
    rows: &[(u32, u8, &[u8])],
) {
    rops.extend_from_slice(&[0x0E, 0x00, input]);
    rops.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for column in columns {
        rops.extend_from_slice(&column.to_le_bytes());
    }
    rops.extend_from_slice(&(rows.len() as u16).to_le_bytes());
    for (row_id, recipient_type, row) in rows {
        rops.extend_from_slice(&row_id.to_le_bytes());
        rops.push(*recipient_type);
        rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
        rops.extend_from_slice(row);
    }
}

fn append_rop_save_changes_message(rops: &mut Vec<u8>, input: u8, response: u8) {
    append_rop_save_changes_message_with_flags(rops, input, response, 0x00);
}

fn append_rop_save_changes_message_with_flags(
    rops: &mut Vec<u8>,
    input: u8,
    response: u8,
    save_flags: u8,
) {
    rops.extend_from_slice(&[0x0C, 0x00, input, response, save_flags]);
}

fn test_conversation_index(conversation_id: Uuid) -> Vec<u8> {
    let mut value = Vec::with_capacity(22);
    value.extend_from_slice(&[0x01, 0, 0, 0, 0, 0]);
    value.extend_from_slice(conversation_id.as_bytes());
    value
}

fn append_rop_sync_manifest_get_buffer(
    rops: &mut Vec<u8>,
    input: u8,
    output: u8,
    buffer_size: u16,
) {
    append_rop_sync_manifest_get_buffer_with_state(rops, input, output, buffer_size, &[]);
}

fn append_rop_sync_manifest_get_buffer_with_state(
    rops: &mut Vec<u8>,
    input: u8,
    output: u8,
    buffer_size: u16,
    state: &[u8],
) {
    rops.extend_from_slice(&[
        0x70, 0x00, input, output, // RopSynchronizationConfigure
        0x01,   // content sync
        0x00,   // SendOptions
        0x28, 0x00, // SynchronizationFlags: ReadState | Normal
        0x00, 0x00, // RestrictionDataSize
        0x05, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN
        0x00, 0x00, // PropertyTagCount
        0x75, 0x00, output, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x4017_0102u32.to_le_bytes());
    rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
    if !state.is_empty() {
        rops.extend_from_slice(&[
            0x76, 0x00, output, // RopSynchronizationUploadStateStreamContinue
        ]);
        rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
        rops.extend_from_slice(state);
    }
    rops.extend_from_slice(&[
        0x77, 0x00, output, // RopSynchronizationUploadStateStreamEnd
    ]);
    if !state.is_empty() {
        rops.extend_from_slice(&[
            0x75, 0x00, output, // RopSynchronizationUploadStateStreamBegin
        ]);
        rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
        rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
        rops.extend_from_slice(&[
            0x76, 0x00, output, // RopSynchronizationUploadStateStreamContinue
        ]);
        rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
        rops.extend_from_slice(state);
        rops.extend_from_slice(&[
            0x77, 0x00, output, // RopSynchronizationUploadStateStreamEnd
        ]);
    }
    rops.extend_from_slice(&[
        0x4E, 0x00, output, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&buffer_size.to_le_bytes());
}

async fn content_sync_response_rops(
    store: FakeStore,
    folder_global_counter: u64,
    client_state: &[u8],
) -> Vec<u8> {
    content_sync_response_rops_for_store(
        store,
        test_mapi_folder_id(folder_global_counter),
        client_state,
    )
    .await
}

async fn content_sync_response_rops_for_store<S>(
    store: S,
    folder_id: u64,
    client_state: &[u8],
) -> Vec<u8>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
{
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    append_rop_sync_manifest_get_buffer_with_state(&mut rops, 1, 2, 4096, client_state);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    response_rops_from_execute_response(response).await
}

fn append_rop_outlook_hierarchy_sync_manifest_get_buffer(
    rops: &mut Vec<u8>,
    input: u8,
    output: u8,
    buffer_size: u16,
) {
    append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state(
        rops,
        input,
        output,
        buffer_size,
        &[],
    );
}

fn append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state(
    rops: &mut Vec<u8>,
    input: u8,
    output: u8,
    buffer_size: u16,
    state: &[u8],
) {
    let buffer_size = buffer_size.max(8192);
    rops.extend_from_slice(&[
        0x70, 0x00, input, output, // RopSynchronizationConfigure
        0x02,   // hierarchy sync
        0x09,   // SendOptions
        0x01, 0x01, // SynchronizationFlags
        0x00, 0x00, // RestrictionDataSize
        0x01, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid
        0x08, 0x00, // PropertyTagCount
        0x03, 0x00, 0x01, 0x36, // PidTagFolderType
        0x03, 0x00, 0x02, 0x36, // PidTagContentCount
        0x03, 0x00, 0x03, 0x36, // PidTagContentUnreadCount
        0x03, 0x00, 0x08, 0x0e, // PidTagMessageSize
        0x03, 0x00, 0xf4, 0x0f, // PidTagAccess
        0x02, 0x01, 0xe0, 0x3f, // PidTagMappingSignature
        0x02, 0x01, 0xe1, 0x3f, // PidTagRecordKey
        0x02, 0x01, 0x27, 0x0e, // PidTagOrdinalMost
        0x75, 0x00, output, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x4017_0003u32.to_le_bytes());
    rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
    if !state.is_empty() {
        rops.extend_from_slice(&[
            0x76, 0x00, output, // RopSynchronizationUploadStateStreamContinue
        ]);
        rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
        rops.extend_from_slice(state);
    }
    rops.extend_from_slice(&[
        0x77, 0x00, output, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, output, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
    if !state.is_empty() {
        rops.extend_from_slice(&[
            0x76, 0x00, output, // RopSynchronizationUploadStateStreamContinue
        ]);
        rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
        rops.extend_from_slice(state);
    }
    rops.extend_from_slice(&[
        0x77, 0x00, output, // RopSynchronizationUploadStateStreamEnd
        0x4E, 0x00, output, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&buffer_size.to_le_bytes());
}

fn append_rop_set_read_flags(rops: &mut Vec<u8>, input: u8, read_flags: u8, message_ids: &[u64]) {
    rops.extend_from_slice(&[0x66, 0x00, input, 0x00, read_flags]);
    rops.extend_from_slice(&(message_ids.len() as u16).to_le_bytes());
    for message_id in message_ids {
        append_mapi_wire_id(rops, *message_id);
    }
}

fn append_rop_open_message(
    rops: &mut Vec<u8>,
    input: u8,
    output: u8,
    folder_id: u64,
    message_id: u64,
) {
    rops.extend_from_slice(&[0x03, 0x00, input, output]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(rops, folder_id);
    rops.push(0);
    append_mapi_wire_id(rops, message_id);
}

fn append_rop_submit_message(rops: &mut Vec<u8>, input: u8) {
    rops.extend_from_slice(&[0x32, 0x00, input, 0x00]);
}

fn append_rop_transport_send(rops: &mut Vec<u8>, input: u8) {
    rops.extend_from_slice(&[0x4A, 0x00, input]);
}

fn append_rop_query_subject_rows(rops: &mut Vec<u8>, input: u8, output: u8, row_count: u16) {
    rops.extend_from_slice(&[
        0x05, 0x00, input, output, 0x00, // RopGetContentsTable
        0x12, 0x00, output, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, output, 0x00, 0x01]);
    rops.extend_from_slice(&row_count.to_le_bytes());
}

async fn response_rops_from_execute_response(response: axum::response::Response) -> Vec<u8> {
    let body = response_bytes(response).await;
    let (response_rops, _) = response_rops_and_handles_from_execute_body(&body);
    response_rops
}

fn response_rops_and_handles_from_execute_body(body: &[u8]) -> (Vec<u8>, Vec<u32>) {
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = rop_buffer[2..2 + response_rop_size].to_vec();
    let response_handles = rop_buffer[2 + response_rop_size..]
        .chunks_exact(4)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
        .collect();
    (response_rops, response_handles)
}

async fn execute_rops_response_rops(rops: &[u8], handles: &[u32]) -> Vec<u8> {
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    });
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(rops, handles)),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    response_rops_from_execute_response(response).await
}

fn mapi_recipient_row(display_name: &str, address: &str, recipient_type: u8) -> Vec<u8> {
    let mut row = Vec::new();
    row.extend_from_slice(&utf16z(display_name));
    row.extend_from_slice(&utf16z(address));
    row.extend_from_slice(&(recipient_type as i32).to_le_bytes());
    row
}

fn mapi_wrapped_recipient_row(display_name: &str, address: &str, recipient_type: u8) -> Vec<u8> {
    let mut row = Vec::new();
    row.extend_from_slice(&0x061Bu16.to_le_bytes());
    row.extend_from_slice(&utf16z(address));
    row.extend_from_slice(&utf16z(display_name));
    row.extend_from_slice(&utf16z(display_name));
    row.extend_from_slice(&3u16.to_le_bytes());
    row.push(0);
    row.extend_from_slice(&utf16z(display_name));
    row.extend_from_slice(&utf16z(address));
    row.extend_from_slice(&(recipient_type as i32).to_le_bytes());
    row
}

fn mapi_wrapped_x500_recipient_row(display_name: &str, legacy_dn: &str) -> Vec<u8> {
    let mut row = Vec::new();
    row.extend_from_slice(&0x0651u16.to_le_bytes());
    row.push(1);
    row.push(0);
    row.extend_from_slice(legacy_dn.as_bytes());
    row.push(0);
    row.extend_from_slice(&utf16z(display_name));
    row.extend_from_slice(&utf16z(display_name));
    row.extend_from_slice(&3u16.to_le_bytes());
    row.push(0);
    row.extend_from_slice(&6i32.to_le_bytes());
    row.extend_from_slice(&0i32.to_le_bytes());
    row.extend_from_slice(&utf16z(display_name));
    row
}

fn mapi_content_restriction(property_tag: u32, value: &str) -> Vec<u8> {
    let mut restriction = vec![0x03];
    restriction.extend_from_slice(&0x0001u16.to_le_bytes());
    restriction.extend_from_slice(&0x0001u16.to_le_bytes());
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.extend_from_slice(&utf16z(value));
    restriction
}

fn test_mapi_message_id(id: &str) -> u64 {
    let uuid = Uuid::parse_str(id).unwrap();
    test_mapi_uuid_id(&uuid)
}

fn test_mapi_folder_id(global_counter: u64) -> u64 {
    ((global_counter & 0x0000_FFFF_FFFF_FFFF) << 16) | 1
}

fn globcnt_bytes(value: u64) -> [u8; 6] {
    crate::mapi::identity::globcnt_bytes(value)
}

fn test_mapi_uuid_id(uuid: &Uuid) -> u64 {
    let bytes = uuid.as_bytes();
    let value = u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]) & 0x0000_FFFF_FFFF_FFFF;
    test_mapi_folder_id(value.max(0x100))
}

fn append_rop_get_properties_specific(rops: &mut Vec<u8>, input: u8, property_tags: &[u32]) {
    rops.extend_from_slice(&[0x07, 0x00, input]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&(property_tags.len() as u16).to_le_bytes());
    for tag in property_tags {
        rops.extend_from_slice(&tag.to_le_bytes());
    }
}

fn append_rop_delete_messages(rops: &mut Vec<u8>, input: u8, message_ids: &[u64]) {
    rops.extend_from_slice(&[0x1E, 0x00, input, 0x00, 0x00]);
    rops.extend_from_slice(&(message_ids.len() as u16).to_le_bytes());
    for message_id in message_ids {
        append_mapi_wire_id(rops, *message_id);
    }
}

fn test_filetime(date: &str, time: &str) -> i64 {
    let year = date
        .get(0..4)
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap();
    let month = date
        .get(5..7)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap();
    let day = date
        .get(8..10)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap();
    let hour = time
        .get(0..2)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap();
    let minute = time
        .get(3..5)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap();
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let unix_seconds = days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60;
    (unix_seconds + 11_644_473_600) * 10_000_000
}

fn utf16z_string_bytes(value: &[u8]) -> Vec<u8> {
    value
        .chunks_exact(2)
        .take_while(|chunk| *chunk != [0, 0])
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect::<Vec<_>>()
        .iter()
        .map(|unit| char::from_u32(*unit as u32).unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect::<String>()
        .into_bytes()
}

fn hex_bytes(input: &str) -> Vec<u8> {
    let compact: String = input.chars().filter(|ch| !ch.is_whitespace()).collect();
    assert_eq!(compact.len() % 2, 0);
    compact
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0]);
            let low = hex_nibble(pair[1]);
            (high << 4) | low
        })
        .collect()
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => panic!("invalid hex byte"),
    }
}

#[tokio::test]
async fn fake_store_rejects_invalid_public_folder_permission_rights() {
    let delegate = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let store = FakeStore {
        directory_accounts: Arc::new(Mutex::new(vec![delegate.clone()])),
        ..Default::default()
    };
    let folder_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
    let audit = lpe_storage::AuditEntryInput {
        actor: "alice@example.test".to_string(),
        action: "mapi-modify-public-folder-permissions".to_string(),
        subject: folder_id.to_string(),
    };

    for (may_read, may_write, may_delete, may_share, expected) in [
        (
            false,
            true,
            false,
            false,
            "read access is required when granting write, delete, or share",
        ),
        (
            true,
            false,
            true,
            false,
            "delete access requires write access",
        ),
        (
            true,
            false,
            false,
            true,
            "share access requires write access",
        ),
    ] {
        let error = store
            .upsert_public_folder_permission(
                PublicFolderPermissionInput {
                    account_id: FakeStore::account().account_id,
                    public_folder_id: folder_id,
                    principal_account_id: delegate.account_id,
                    may_read,
                    may_write,
                    may_delete,
                    may_share,
                },
                audit.clone(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), expected);
    }

    assert!(store.public_folder_permissions.lock().unwrap().is_empty());
    assert!(store
        .mapi_folder_permission_audits
        .lock()
        .unwrap()
        .is_empty());
}

mod ews;
mod mapi_over_http;
mod rpc_proxy;
