use std::{env, str::FromStr, sync::OnceLock, time::Duration};

use anyhow::{Context, Result};
use lpe_storage::{
    AttachmentUploadInput, MapiEventAttachmentChanges, MapiEventAttachmentUpsert,
    MapiEventCommitInput, MapiEventCommitOutcome, MapiEventCreateInput,
    MapiEventCustomPropertyValue, MapiEventReminderPatch, Storage, UpsertClientEventInput,
};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool, Row,
};
use uuid::Uuid;

const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");
const REPLICA_GUID: [u8; 16] = [
    0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f, 0x10,
];
const FIRST_RESERVED_HIGH_GLOBAL_COUNTER: u64 = 0x7FFF_FE00_0000;
static DATABASE_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn database_test_lock() -> &'static tokio::sync::Mutex<()> {
    DATABASE_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

struct EventFixture {
    storage: Storage,
    admin_pool: PgPool,
    schema_name: String,
    tenant_id: Uuid,
    account_id: Uuid,
    calendar_id: Uuid,
    event_id: Uuid,
}

impl EventFixture {
    async fn cleanup(self) -> Result<()> {
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

async fn event_fixture() -> Result<Option<EventFixture>> {
    let Some(database_url) = env::var("TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!(
            "skipping PostgreSQL-backed MAPI Event commit test; TEST_DATABASE_URL is not set"
        );
        return Ok(None);
    };

    let schema_name = format!("lpe_mapi_event_{}", Uuid::new_v4().simple());
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(&database_url)?)
        .await
        .context("connect to TEST_DATABASE_URL")?;
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
    sqlx::raw_sql(SCHEMA_SQL).execute(&pool).await?;

    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    let account_id = Uuid::new_v4();
    let calendar_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, slug, display_name) VALUES ($1, $2, 'MAPI Event')")
        .bind(tenant_id)
        .bind(format!("mapi-event-{}", tenant_id.simple()))
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO domains (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(domain_id)
        .bind(tenant_id)
        .bind(format!("{}.example.test", tenant_id.simple()))
        .execute(&pool)
        .await?;
    sqlx::query(
        "INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name) \
         VALUES ($1, $2, $3, $4, 'Alice')",
    )
    .bind(account_id)
    .bind(tenant_id)
    .bind(domain_id)
    .bind(format!("alice@{}.example.test", tenant_id.simple()))
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO calendars (id, tenant_id, owner_account_id, display_name, role, sync_modseq) \
         VALUES ($1, $2, $3, 'Calendar', 'calendar', 7)",
    )
    .bind(calendar_id)
    .bind(tenant_id)
    .bind(account_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO calendar_events (
            id, tenant_id, owner_account_id, calendar_id, uid, title,
            starts_at, ends_at, time_zone, modseq
        )
        VALUES (
            $1, $2, $3, $4, 'event-uid', 'Before',
            '2026-07-15T08:00:00Z', '2026-07-15T09:00:00Z', 'UTC', 7
        )
        "#,
    )
    .bind(event_id)
    .bind(tenant_id)
    .bind(account_id)
    .bind(calendar_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO account_sync_state (tenant_id, account_id, category, current_modseq) \
         VALUES ($1, $2, 'calendar', 7)",
    )
    .bind(tenant_id)
    .bind(account_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO mapi_mailbox_replicas \
         (tenant_id, account_id, replica_guid, next_global_counter) VALUES ($1, $2, $3, 100)",
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(Uuid::from_bytes(REPLICA_GUID))
    .execute(&pool)
    .await?;
    let source_key = change_key(50);
    let initial_pcl = predecessor_change_list(&source_key);
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id,
            mapi_global_counter, mapi_object_id, source_key, change_key,
            instance_key, mapi_change_number, predecessor_change_list
        )
        VALUES ($1, $2, 'calendar_event', $3, 50, $4, $5, $5, $5, 50, $6)
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(event_id)
    .bind(mapi_store_id(50) as i64)
    .bind(&source_key)
    .bind(&initial_pcl)
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO mapi_custom_property_values (
            tenant_id, account_id, object_kind, canonical_id,
            property_tag, property_type, property_value
        )
        VALUES ($1, $2, 'calendar_event', $3, $4, $5, $6)
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(event_id)
    .bind(i64::from(0x8001_001Fu32))
    .bind(0x001Fi32)
    .bind(b"old".as_slice())
    .execute(&pool)
    .await?;

    Ok(Some(EventFixture {
        storage: Storage::new(pool),
        admin_pool,
        schema_name,
        tenant_id,
        account_id,
        calendar_id,
        event_id,
    }))
}

fn updated_event(fixture: &EventFixture, title: &str) -> UpsertClientEventInput {
    UpsertClientEventInput {
        id: Some(fixture.event_id),
        account_id: fixture.account_id,
        uid: "event-uid".to_string(),
        date: "2026-07-15".to_string(),
        time: "10:11".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 45,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 2,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: title.to_string(),
        location: "Geneva".to_string(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "{}".to_string(),
        notes: "Canonical notes".to_string(),
        body_html: "<p>Canonical notes</p>".to_string(),
    }
}

fn commit_input(fixture: &EventFixture, title: &str) -> MapiEventCommitInput {
    MapiEventCommitInput {
        principal_account_id: fixture.account_id,
        event_id: fixture.event_id,
        expected_modseq: 7,
        force_save: false,
        event: Some(updated_event(fixture, title)),
        reminder: MapiEventReminderPatch {
            reminder_set: Some(true),
            reminder_at: Some("2026-07-15T10:01:00Z".to_string()),
            reminder_dismissed_at: None,
        },
        custom_property_upserts: vec![MapiEventCustomPropertyValue {
            property_tag: 0x8002_001F,
            property_type: 0x001F,
            property_value: b"new".to_vec(),
        }],
        custom_property_deletes: vec![0x8001_001F],
        attachment_changes: MapiEventAttachmentChanges::default(),
    }
}

fn create_input(
    principal_account_id: Uuid,
    collection_id: impl Into<String>,
    event_id: Uuid,
    title: &str,
) -> MapiEventCreateInput {
    MapiEventCreateInput {
        principal_account_id,
        collection_id: collection_id.into(),
        event: UpsertClientEventInput {
            id: Some(event_id),
            account_id: principal_account_id,
            uid: format!("mapi-goid:{}", event_id.simple()),
            date: "2027-01-15".to_string(),
            time: "10:11".to_string(),
            time_zone: "Europe/Zurich".to_string(),
            duration_minutes: 45,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: title.to_string(),
            location: "Salle Genève".to_string(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "{}".to_string(),
            notes: "Création Outlook atomique".to_string(),
            body_html: "<p>Création Outlook atomique</p>".to_string(),
        },
        reminder: MapiEventReminderPatch {
            reminder_set: Some(true),
            reminder_at: Some("2027-01-15T08:56:00Z".to_string()),
            reminder_dismissed_at: None,
        },
        custom_property_upserts: vec![MapiEventCustomPropertyValue {
            property_tag: 0x8002_001F,
            property_type: 0x001F,
            property_value: b"opaque Outlook category".to_vec(),
        }],
        attachment_changes: MapiEventAttachmentChanges::default(),
    }
}

fn attachment_upsert(attach_num: u32, file_name: &str) -> MapiEventAttachmentUpsert {
    MapiEventAttachmentUpsert {
        attach_num,
        attachment: AttachmentUploadInput {
            file_name: file_name.to_string(),
            media_type: "application/pdf".to_string(),
            disposition: Some("attachment".to_string()),
            content_id: None,
            blob_bytes: b"%PDF-1.7\ncalendar attachment\n".to_vec(),
        },
        custom_property_upserts: vec![MapiEventCustomPropertyValue {
            property_tag: 0x8003_001F,
            property_type: 0x001F,
            property_value: b"attachment metadata".to_vec(),
        }],
    }
}

#[tokio::test]
async fn mapi_event_commit_persists_one_atomic_version() -> Result<()> {
    let _guard = database_test_lock().lock().await;
    let Some(fixture) = event_fixture().await? else {
        return Ok(());
    };

    let outcome = fixture
        .storage
        .commit_mapi_event_update(commit_input(&fixture, "After"))
        .await?;
    let MapiEventCommitOutcome::Saved(saved) = outcome else {
        panic!("expected saved MAPI Event outcome");
    };
    let version = saved.version;
    assert_eq!(version.event_id, fixture.event_id);
    assert_eq!(version.canonical_modseq, 8);
    assert_eq!(version.change_number, 100);
    assert_eq!(version.change_key, change_key(100));

    let event = sqlx::query(
        "SELECT title, reminder_set, reminder_at IS NOT NULL AS has_reminder_at, modseq \
         FROM calendar_events WHERE id = $1",
    )
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(event.get::<String, _>("title"), "After");
    assert!(event.get::<bool, _>("reminder_set"));
    assert!(event.get::<bool, _>("has_reminder_at"));
    assert_eq!(event.get::<i64, _>("modseq"), 8);

    let custom_tags = sqlx::query_scalar::<_, i64>(
        "SELECT property_tag FROM mapi_custom_property_values \
         WHERE account_id = $1 AND canonical_id = $2 ORDER BY property_tag",
    )
    .bind(fixture.account_id)
    .bind(fixture.event_id)
    .fetch_all(fixture.storage.pool())
    .await?;
    assert_eq!(custom_tags, vec![i64::from(0x8002_001Fu32)]);

    let identity = sqlx::query(
        "SELECT mapi_global_counter, mapi_object_id, source_key, instance_key, \
                mapi_change_number, change_key, predecessor_change_list \
         FROM mapi_object_identities WHERE account_id = $1 AND canonical_id = $2",
    )
    .bind(fixture.account_id)
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(identity.get::<i64, _>("mapi_global_counter"), 50);
    assert_eq!(
        identity.get::<i64, _>("mapi_object_id"),
        mapi_store_id(50) as i64
    );
    assert_eq!(identity.get::<Vec<u8>, _>("source_key"), change_key(50));
    assert_eq!(identity.get::<Vec<u8>, _>("instance_key"), change_key(50));
    assert_eq!(identity.get::<i64, _>("mapi_change_number"), 100);
    assert_eq!(identity.get::<Vec<u8>, _>("change_key"), change_key(100));
    assert_eq!(
        identity.get::<Vec<u8>, _>("predecessor_change_list"),
        predecessor_change_list(&change_key(100))
    );

    let mail_change_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM mail_change_log WHERE object_kind = 'calendar_event' AND object_id = $1",
    )
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(mail_change_count, 1);
    let journal_categories = sqlx::query_scalar::<_, String>(
        "SELECT category FROM canonical_change_journal WHERE tenant_id = $1 ORDER BY sequence",
    )
    .bind(fixture.tenant_id)
    .fetch_all(fixture.storage.pool())
    .await?;
    assert_eq!(journal_categories, vec!["calendar", "rights"]);
    let fetched_versions = fixture
        .storage
        .fetch_mapi_event_versions(fixture.account_id, &[fixture.event_id])
        .await?;
    assert_eq!(fetched_versions, vec![version]);

    fixture.cleanup().await
}

#[tokio::test]
async fn mapi_event_commit_updated_at_advances_after_waiting_for_a_row_lock() -> Result<()> {
    let _guard = database_test_lock().lock().await;
    let Some(fixture) = event_fixture().await? else {
        return Ok(());
    };

    let first = fixture
        .storage
        .commit_mapi_event_update(commit_input(&fixture, "First timestamped version"))
        .await?;
    let MapiEventCommitOutcome::Saved(first) = first else {
        panic!("expected first saved MAPI Event outcome");
    };

    let mut blocker = fixture.storage.pool().begin().await?;
    let blocker_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await?;
    sqlx::query("SELECT id FROM calendar_events WHERE id = $1 FOR UPDATE")
        .bind(fixture.event_id)
        .fetch_one(&mut *blocker)
        .await?;

    let mut second_input = commit_input(&fixture, "Version saved after the row lock");
    second_input.expected_modseq = first.version.canonical_modseq;
    let waiting_storage = fixture.storage.clone();
    let waiting_commit =
        tokio::spawn(async move { waiting_storage.commit_mapi_event_update(second_input).await });

    let waiting_transaction_started_at = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let started_at = sqlx::query_scalar::<_, String>(
                r#"
                SELECT to_char(
                    activity.xact_start AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                )
                FROM pg_stat_activity activity
                WHERE $1 = ANY(pg_blocking_pids(activity.pid))
                  AND activity.wait_event_type = 'Lock'
                  AND activity.query LIKE '%FOR UPDATE OF event%'
                ORDER BY activity.xact_start
                LIMIT 1
                "#,
            )
            .bind(blocker_pid)
            .fetch_optional(fixture.storage.pool())
            .await?;
            if let Some(started_at) = started_at {
                return Ok::<_, sqlx::Error>(started_at);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("second MAPI Event commit did not wait for the row lock")??;

    let concurrent_updated_at = sqlx::query_scalar::<_, String>(
        r#"
        UPDATE calendar_events
        SET updated_at = clock_timestamp()
        WHERE id = $1
        RETURNING to_char(
            updated_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
        "#,
    )
    .bind(fixture.event_id)
    .fetch_one(&mut *blocker)
    .await?;
    assert!(
        waiting_transaction_started_at < concurrent_updated_at,
        "the regression requires the waiting transaction to predate the concurrent timestamp"
    );
    blocker.commit().await?;

    let second = tokio::time::timeout(Duration::from_secs(5), waiting_commit)
        .await
        .context("waiting MAPI Event commit did not finish after releasing the row lock")??;
    let MapiEventCommitOutcome::Saved(second) = second? else {
        panic!("expected second saved MAPI Event outcome");
    };
    assert_eq!(second.version.canonical_modseq, 9);
    assert!(first.version.updated_at < concurrent_updated_at);
    assert!(
        concurrent_updated_at < second.version.updated_at,
        "[MS-OXCMSG] section 3.2.5.3 save version timestamps must remain monotone"
    );

    let persisted_updated_at = sqlx::query_scalar::<_, String>(
        r#"
        SELECT to_char(
            updated_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
        FROM calendar_events
        WHERE id = $1
        "#,
    )
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(persisted_updated_at, second.version.updated_at);

    fixture.cleanup().await
}

#[tokio::test]
async fn mapi_event_commit_persists_subject_and_attachment_with_one_change() -> Result<()> {
    let _guard = database_test_lock().lock().await;
    let Some(fixture) = event_fixture().await? else {
        return Ok(());
    };
    let mut input = commit_input(&fixture, "Sujet et pièce jointe atomiques");
    input.attachment_changes.upserts = vec![attachment_upsert(0, "ordre-du-jour.pdf")];

    let outcome = fixture.storage.commit_mapi_event_update(input).await?;
    let MapiEventCommitOutcome::Saved(saved) = outcome else {
        panic!("expected saved MAPI Event outcome");
    };

    assert_eq!(saved.version.canonical_modseq, 8);
    assert_eq!(saved.version.change_number, 100);
    assert_eq!(saved.attachments.len(), 1);
    assert_eq!(saved.attachments[0].file_name, "ordre-du-jour.pdf");
    let persisted = sqlx::query(
        "SELECT event.title, event.modseq, attachment.file_name, attachment.ordinal \
         FROM calendar_events event \
         JOIN calendar_event_attachments attachment ON attachment.event_id = event.id \
         WHERE event.id = $1",
    )
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(
        persisted.get::<String, _>("title"),
        "Sujet et pièce jointe atomiques"
    );
    assert_eq!(persisted.get::<i64, _>("modseq"), 8);
    assert_eq!(persisted.get::<String, _>("file_name"), "ordre-du-jour.pdf");
    assert_eq!(persisted.get::<i32, _>("ordinal"), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mail_change_log \
             WHERE object_kind = 'calendar_event' AND object_id = $1"
        )
        .bind(fixture.event_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mapi_custom_property_values \
             WHERE object_kind = 'attachment' AND canonical_id = $1"
        )
        .bind(saved.attachments[0].id)
        .fetch_one(fixture.storage.pool())
        .await?,
        1
    );

    fixture.cleanup().await
}

#[tokio::test]
async fn mapi_event_attachment_failure_rolls_back_parent_and_blob() -> Result<()> {
    let _guard = database_test_lock().lock().await;
    let Some(fixture) = event_fixture().await? else {
        return Ok(());
    };
    let mut input = commit_input(&fixture, "Ne doit jamais persister");
    input.attachment_changes.upserts = vec![attachment_upsert(0, "rollback.pdf")];
    input.attachment_changes.delete_attachment_ids = vec![Uuid::new_v4()];

    let result = fixture.storage.commit_mapi_event_update(input).await;
    assert!(result.is_err());

    let event =
        sqlx::query("SELECT title, reminder_set, modseq FROM calendar_events WHERE id = $1")
            .bind(fixture.event_id)
            .fetch_one(fixture.storage.pool())
            .await?;
    assert_eq!(event.get::<String, _>("title"), "Before");
    assert!(!event.get::<bool, _>("reminder_set"));
    assert_eq!(event.get::<i64, _>("modseq"), 7);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM calendar_event_attachments WHERE event_id = $1"
        )
        .bind(fixture.event_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM blobs WHERE tenant_id = $1")
            .bind(fixture.tenant_id)
            .fetch_one(fixture.storage.pool())
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM mail_change_log WHERE object_id = $1")
            .bind(fixture.event_id)
            .fetch_one(fixture.storage.pool())
            .await?,
        0
    );

    fixture.cleanup().await
}

#[tokio::test]
async fn canonical_event_writer_advances_the_persisted_mapi_version() -> Result<()> {
    let _guard = database_test_lock().lock().await;
    let Some(fixture) = event_fixture().await? else {
        return Ok(());
    };

    fixture
        .storage
        .upsert_client_event(updated_event(&fixture, "Updated through canonical API"))
        .await?;

    let event_modseq =
        sqlx::query_scalar::<_, i64>("SELECT modseq FROM calendar_events WHERE id = $1")
            .bind(fixture.event_id)
            .fetch_one(fixture.storage.pool())
            .await?;
    assert_eq!(event_modseq, 8);
    let identity = sqlx::query(
        "SELECT mapi_global_counter, mapi_change_number, change_key, predecessor_change_list \
         FROM mapi_object_identities WHERE account_id = $1 AND canonical_id = $2",
    )
    .bind(fixture.account_id)
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(identity.get::<i64, _>("mapi_global_counter"), 50);
    assert_eq!(identity.get::<i64, _>("mapi_change_number"), 100);
    assert_eq!(identity.get::<Vec<u8>, _>("change_key"), change_key(100));
    assert_eq!(
        identity.get::<Vec<u8>, _>("predecessor_change_list"),
        predecessor_change_list(&change_key(100))
    );

    fixture.cleanup().await
}

#[tokio::test]
async fn mapi_event_commit_rejects_stale_version_unless_force_save() -> Result<()> {
    let _guard = database_test_lock().lock().await;
    let Some(fixture) = event_fixture().await? else {
        return Ok(());
    };

    let first = fixture
        .storage
        .commit_mapi_event_update(commit_input(&fixture, "First writer"))
        .await?;
    assert!(matches!(first, MapiEventCommitOutcome::Saved(_)));

    let stale = fixture
        .storage
        .commit_mapi_event_update(commit_input(&fixture, "Stale writer"))
        .await?;
    assert_eq!(
        stale,
        MapiEventCommitOutcome::ObjectModified { current_modseq: 8 }
    );
    let unchanged = sqlx::query("SELECT title, modseq FROM calendar_events WHERE id = $1")
        .bind(fixture.event_id)
        .fetch_one(fixture.storage.pool())
        .await?;
    assert_eq!(unchanged.get::<String, _>("title"), "First writer");
    assert_eq!(unchanged.get::<i64, _>("modseq"), 8);
    let counter = sqlx::query_scalar::<_, i64>(
        "SELECT next_global_counter FROM mapi_mailbox_replicas WHERE account_id = $1",
    )
    .bind(fixture.account_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(counter, 101);

    let mut forced_input = commit_input(&fixture, "Forced writer");
    forced_input.force_save = true;
    let forced = fixture
        .storage
        .commit_mapi_event_update(forced_input)
        .await?;
    let MapiEventCommitOutcome::Saved(saved) = forced else {
        panic!("ForceSave must override only the stale-version guard");
    };
    let version = saved.version;
    assert_eq!(version.canonical_modseq, 9);
    assert_eq!(version.change_number, 101);

    fixture.cleanup().await
}

#[tokio::test]
async fn mapi_event_commit_rolls_back_when_change_number_allocation_fails() -> Result<()> {
    let _guard = database_test_lock().lock().await;
    let Some(fixture) = event_fixture().await? else {
        return Ok(());
    };
    sqlx::query("UPDATE mapi_mailbox_replicas SET next_global_counter = $2 WHERE account_id = $1")
        .bind(fixture.account_id)
        .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
        .execute(fixture.storage.pool())
        .await?;

    let result = fixture
        .storage
        .commit_mapi_event_update(commit_input(&fixture, "Must roll back"))
        .await;
    assert!(result.is_err());

    let event =
        sqlx::query("SELECT title, reminder_set, modseq FROM calendar_events WHERE id = $1")
            .bind(fixture.event_id)
            .fetch_one(fixture.storage.pool())
            .await?;
    assert_eq!(event.get::<String, _>("title"), "Before");
    assert!(!event.get::<bool, _>("reminder_set"));
    assert_eq!(event.get::<i64, _>("modseq"), 7);
    let custom_tags = sqlx::query_scalar::<_, i64>(
        "SELECT property_tag FROM mapi_custom_property_values WHERE canonical_id = $1",
    )
    .bind(fixture.event_id)
    .fetch_all(fixture.storage.pool())
    .await?;
    assert_eq!(custom_tags, vec![i64::from(0x8001_001Fu32)]);
    let identity_change_number = sqlx::query_scalar::<_, i64>(
        "SELECT mapi_change_number FROM mapi_object_identities WHERE canonical_id = $1",
    )
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(identity_change_number, 50);
    let mail_change_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM mail_change_log WHERE object_id = $1")
            .bind(fixture.event_id)
            .fetch_one(fixture.storage.pool())
            .await?;
    assert_eq!(mail_change_count, 0);

    fixture.cleanup().await
}

#[tokio::test]
async fn mapi_event_create_rolls_back_every_artifact_and_retry_creates_one_event() -> Result<()> {
    let _guard = database_test_lock().lock().await;
    let Some(fixture) = event_fixture().await? else {
        return Ok(());
    };
    let created_event_id = Uuid::new_v4();
    let input = create_input(
        fixture.account_id,
        "default",
        created_event_id,
        "Création atomique puis retry",
    );
    sqlx::query("UPDATE mapi_mailbox_replicas SET next_global_counter = $2 WHERE account_id = $1")
        .bind(fixture.account_id)
        .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
        .execute(fixture.storage.pool())
        .await?;

    let failed = fixture.storage.create_mapi_event(input.clone()).await;
    assert!(failed.is_err());
    for (table, predicate) in [
        ("calendar_events", "id = $1"),
        ("mapi_object_identities", "canonical_id = $1"),
        ("mapi_custom_property_values", "canonical_id = $1"),
        ("mail_change_log", "object_id = $1"),
    ] {
        let count = sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*) FROM {table} WHERE {predicate}"
        ))
        .bind(created_event_id)
        .fetch_one(fixture.storage.pool())
        .await?;
        assert_eq!(count, 0, "{table} retained a partial MAPI Event create");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT current_modseq FROM account_sync_state WHERE account_id = $1 AND category = 'calendar'",
        )
        .bind(fixture.account_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        7
    );

    sqlx::query("UPDATE mapi_mailbox_replicas SET next_global_counter = 150 WHERE account_id = $1")
        .bind(fixture.account_id)
        .execute(fixture.storage.pool())
        .await?;
    let created = fixture.storage.create_mapi_event(input).await?;

    assert_eq!(created.event.id, created_event_id);
    assert_eq!(created.mapi_object_id, mapi_store_id(150));
    assert_eq!(created.version.event_id, created_event_id);
    assert_eq!(created.version.canonical_modseq, 8);
    assert_eq!(created.version.change_number, 150);
    assert_eq!(
        created.reminder.reminder_at.as_deref(),
        Some("2027-01-15T08:56:00Z")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM calendar_events WHERE id = $1")
            .bind(created_event_id)
            .fetch_one(fixture.storage.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mail_change_log WHERE object_kind = 'calendar_event' AND object_id = $1",
        )
        .bind(created_event_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT change_kind FROM mail_change_log WHERE object_kind = 'calendar_event' AND object_id = $1",
        )
        .bind(created_event_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        "created"
    );

    fixture.cleanup().await
}

#[tokio::test]
async fn delegated_mapi_event_create_uses_owner_scope_for_event_and_custom_properties() -> Result<()>
{
    let _guard = database_test_lock().lock().await;
    let Some(fixture) = event_fixture().await? else {
        return Ok(());
    };
    let delegate_account_id = Uuid::new_v4();
    sqlx::query("UPDATE calendars SET role = 'custom' WHERE id = $1")
        .bind(fixture.calendar_id)
        .execute(fixture.storage.pool())
        .await?;
    sqlx::query(
        r#"
        INSERT INTO accounts (
            id, tenant_id, primary_domain_id, primary_email, display_name
        )
        SELECT $1, tenant_id, primary_domain_id, $2, 'Délégué Outlook'
        FROM accounts
        WHERE id = $3
        "#,
    )
    .bind(delegate_account_id)
    .bind(format!(
        "delegate-{}@example.test",
        delegate_account_id.simple()
    ))
    .bind(fixture.account_id)
    .execute(fixture.storage.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO calendar_grants (
            id, tenant_id, calendar_id, owner_account_id, grantee_account_id,
            may_read, may_write, may_delete, may_share
        )
        VALUES ($1, $2, $3, $4, $5, TRUE, TRUE, FALSE, FALSE)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(fixture.tenant_id)
    .bind(fixture.calendar_id)
    .bind(fixture.account_id)
    .bind(delegate_account_id)
    .execute(fixture.storage.pool())
    .await?;
    sqlx::query(
        "INSERT INTO mapi_mailbox_replicas \
         (tenant_id, account_id, replica_guid, next_global_counter) VALUES ($1, $2, $3, 200)",
    )
    .bind(fixture.tenant_id)
    .bind(delegate_account_id)
    .bind(Uuid::from_bytes(REPLICA_GUID))
    .execute(fixture.storage.pool())
    .await?;
    let created_event_id = Uuid::new_v4();

    let created = fixture
        .storage
        .create_mapi_event(create_input(
            delegate_account_id,
            fixture.calendar_id.to_string(),
            created_event_id,
            "Rendez-vous du calendrier partagé",
        ))
        .await?;

    assert_eq!(created.event.owner_account_id, fixture.account_id);
    assert_eq!(created.event.collection_id, fixture.calendar_id.to_string());
    assert_eq!(created.mapi_object_id, mapi_store_id(200));
    let persisted_owner =
        sqlx::query_scalar::<_, Uuid>("SELECT owner_account_id FROM calendar_events WHERE id = $1")
            .bind(created_event_id)
            .fetch_one(fixture.storage.pool())
            .await?;
    assert_eq!(persisted_owner, fixture.account_id);
    let identity_account = sqlx::query_scalar::<_, Uuid>(
        "SELECT account_id FROM mapi_object_identities WHERE object_kind = 'calendar_event' AND canonical_id = $1",
    )
    .bind(created_event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(identity_account, delegate_account_id);
    let custom_property_account = sqlx::query_scalar::<_, Uuid>(
        "SELECT account_id FROM mapi_custom_property_values WHERE object_kind = 'calendar_event' AND canonical_id = $1",
    )
    .bind(created_event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(custom_property_account, fixture.account_id);
    let mut affected_principals = sqlx::query_scalar::<_, Vec<Uuid>>(
        "SELECT affected_principal_ids FROM mail_change_log WHERE object_kind = 'calendar_event' AND object_id = $1",
    )
    .bind(created_event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    affected_principals.sort();
    let mut expected_principals = vec![fixture.account_id, delegate_account_id];
    expected_principals.sort();
    assert_eq!(affected_principals, expected_principals);

    fixture.cleanup().await
}

#[tokio::test]
async fn calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity(
) -> Result<()> {
    let _guard = database_test_lock().lock().await;
    let Some(fixture) = event_fixture().await? else {
        return Ok(());
    };
    let delegate_account_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO accounts (
            id, tenant_id, primary_domain_id, primary_email, display_name
        )
        SELECT $1, tenant_id, primary_domain_id, $2, 'Zoë Müller'
        FROM accounts
        WHERE id = $3
        "#,
    )
    .bind(delegate_account_id)
    .bind(format!("zoe-{}@example.test", delegate_account_id.simple()))
    .bind(fixture.account_id)
    .execute(fixture.storage.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO calendar_grants (
            id, tenant_id, calendar_id, owner_account_id, grantee_account_id,
            may_read, may_write, may_delete, may_share
        )
        VALUES ($1, $2, $3, $4, $5, TRUE, TRUE, FALSE, FALSE)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(fixture.tenant_id)
    .bind(fixture.calendar_id)
    .bind(fixture.account_id)
    .bind(delegate_account_id)
    .execute(fixture.storage.pool())
    .await?;
    sqlx::query(
        "INSERT INTO mapi_mailbox_replicas \
         (tenant_id, account_id, replica_guid, next_global_counter) \
         VALUES ($1, $2, $3, 200)",
    )
    .bind(fixture.tenant_id)
    .bind(delegate_account_id)
    .bind(Uuid::from_bytes(REPLICA_GUID))
    .execute(fixture.storage.pool())
    .await?;
    let delegate_source_key = change_key(60);
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id,
            mapi_global_counter, mapi_object_id, source_key, change_key,
            instance_key, mapi_change_number, predecessor_change_list
        )
        VALUES ($1, $2, 'calendar_event', $3, 60, $4, $5, $5, $5, 60, $6)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(delegate_account_id)
    .bind(fixture.event_id)
    .bind(mapi_store_id(60) as i64)
    .bind(&delegate_source_key)
    .bind(predecessor_change_list(&delegate_source_key))
    .execute(fixture.storage.pool())
    .await?;

    let denied = fixture
        .storage
        .move_accessible_event_to_deleted_items(delegate_account_id, fixture.event_id)
        .await
        .expect_err("delegate without may_delete must not move an event");
    assert!(denied
        .to_string()
        .contains("delete access is not granted on this calendar"));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle_state FROM calendar_events WHERE id = $1"
        )
        .bind(fixture.event_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        "active"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM mail_change_log WHERE object_id = $1")
            .bind(fixture.event_id)
            .fetch_one(fixture.storage.pool())
            .await?,
        0
    );
    sqlx::query(
        "UPDATE calendar_grants SET may_delete = TRUE \
         WHERE calendar_id = $1 AND grantee_account_id = $2",
    )
    .bind(fixture.calendar_id)
    .bind(delegate_account_id)
    .execute(fixture.storage.pool())
    .await?;

    let mut update = commit_input(&fixture, "Rendez-vous à Genève 🌍");
    let event = update.event.as_mut().expect("event update");
    event.recurrence_rule = "FREQ=WEEKLY;BYDAY=MO,WE;COUNT=6".to_string();
    event.recurrence_json = serde_json::json!({
        "frequency": "weekly",
        "byDay": ["mo", "we"],
        "count": 6
    })
    .to_string();
    event.organizer_json = serde_json::json!({
        "email": "alice@example.test",
        "common_name": "Alice Élodie"
    })
    .to_string();
    event.attendees_json = serde_json::json!({
        "organizer": {
            "email": "alice@example.test",
            "common_name": "Alice Élodie"
        },
        "attendees": [{
            "email": "zoe@example.test",
            "common_name": "Zoë Müller",
            "role": "REQ-PARTICIPANT",
            "partstat": "accepted",
            "rsvp": true
        }]
    })
    .to_string();
    update.attachment_changes.upserts = vec![attachment_upsert(0, "ordre-du-jour-été.pdf")];
    let saved = fixture.storage.commit_mapi_event_update(update).await?;
    assert!(matches!(saved, MapiEventCommitOutcome::Saved(_)));

    let moved = fixture
        .storage
        .move_accessible_event_to_deleted_items(fixture.account_id, fixture.event_id)
        .await?;
    assert_eq!(moved.event.id, fixture.event_id);
    assert_eq!(moved.event.title, "Rendez-vous à Genève 🌍");
    assert_eq!(
        moved.event.recurrence_rule,
        "FREQ=WEEKLY;BYDAY=MO,WE;COUNT=6"
    );
    assert!(moved.event.attendees_json.contains("Zoë Müller"));
    let identity = moved
        .principal_identity
        .expect("the principal had an active MAPI identity");
    assert_eq!(identity.account_id, fixture.account_id);
    assert_eq!(identity.old_mapi_object_id, mapi_store_id(50));
    assert_eq!(identity.new_mapi_object_id, mapi_store_id(101));
    assert_eq!(identity.old_source_key, change_key(50));
    assert_eq!(identity.new_source_key, change_key(101));
    assert_eq!(identity.old_change_number, 100);
    assert_eq!(identity.new_change_number, 101);
    assert_eq!(identity.old_change_key, change_key(100));
    assert_eq!(identity.new_change_key, change_key(101));

    let owner_deleted_versions = fixture
        .storage
        .fetch_mapi_event_versions(fixture.account_id, &[fixture.event_id])
        .await?;
    assert_eq!(owner_deleted_versions.len(), 1);
    assert_eq!(owner_deleted_versions[0].event_id, fixture.event_id);
    assert_eq!(owner_deleted_versions[0].change_number, 101);
    assert_eq!(owner_deleted_versions[0].change_key, change_key(101));
    assert_eq!(
        owner_deleted_versions[0].predecessor_change_list,
        predecessor_change_list(&change_key(101))
    );
    let persisted_deleted_version = sqlx::query(
        r#"
        SELECT
            modseq,
            to_char(
                updated_at AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            ) AS updated_at
        FROM calendar_events
        WHERE id = $1
        "#,
    )
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(
        owner_deleted_versions[0].canonical_modseq,
        persisted_deleted_version.get::<i64, _>("modseq")
    );
    assert_eq!(
        owner_deleted_versions[0].updated_at,
        persisted_deleted_version.get::<String, _>("updated_at")
    );

    let delegate_deleted_versions = fixture
        .storage
        .fetch_mapi_event_versions(delegate_account_id, &[fixture.event_id])
        .await?;
    assert_eq!(delegate_deleted_versions.len(), 1);
    assert_eq!(delegate_deleted_versions[0].event_id, fixture.event_id);
    assert_eq!(delegate_deleted_versions[0].change_number, 201);
    assert_eq!(delegate_deleted_versions[0].change_key, change_key(201));
    assert_eq!(
        delegate_deleted_versions[0].predecessor_change_list,
        predecessor_change_list(&change_key(201))
    );
    assert_eq!(
        delegate_deleted_versions[0].updated_at,
        owner_deleted_versions[0].updated_at
    );

    sqlx::query(
        "DELETE FROM calendar_grants \
         WHERE calendar_id = $1 AND grantee_account_id = $2",
    )
    .bind(fixture.calendar_id)
    .bind(delegate_account_id)
    .execute(fixture.storage.pool())
    .await?;
    assert!(fixture
        .storage
        .fetch_mapi_event_versions(delegate_account_id, &[fixture.event_id])
        .await?
        .is_empty());

    assert!(fixture
        .storage
        .fetch_accessible_events(fixture.account_id)
        .await?
        .is_empty());
    let deleted = fixture
        .storage
        .fetch_accessible_deleted_events(fixture.account_id)
        .await?;
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].id, fixture.event_id);
    assert_eq!(deleted[0].title, "Rendez-vous à Genève 🌍");
    assert!(deleted[0].attendees_json.contains("Zoë Müller"));

    let lifecycle = sqlx::query(
        "SELECT lifecycle_state, deleted_at IS NOT NULL AS has_deleted_at, reminder_set, \
                reminder_at IS NOT NULL AS has_reminder_at \
         FROM calendar_events WHERE id = $1",
    )
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(lifecycle.get::<String, _>("lifecycle_state"), "deleted");
    assert!(lifecycle.get::<bool, _>("has_deleted_at"));
    assert!(lifecycle.get::<bool, _>("reminder_set"));
    assert!(lifecycle.get::<bool, _>("has_reminder_at"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM calendar_event_attachments WHERE event_id = $1"
        )
        .bind(fixture.event_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        1
    );
    assert!(fixture
        .storage
        .fetch_calendar_event_attachments(fixture.account_id, fixture.event_id)
        .await?
        .is_empty());
    let deleted_attachments = fixture
        .storage
        .fetch_calendar_attachments_for_events(fixture.account_id, &[fixture.event_id])
        .await?;
    assert_eq!(deleted_attachments.len(), 1);
    assert_eq!(deleted_attachments[0].1.len(), 1);
    assert_eq!(
        deleted_attachments[0].1[0].file_name,
        "ordre-du-jour-été.pdf"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mapi_custom_property_values \
             WHERE object_kind = 'calendar_event' AND canonical_id = $1"
        )
        .bind(fixture.event_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        1
    );

    let changes = sqlx::query(
        r#"
        SELECT object_kind, change_kind
        FROM mail_change_log
        WHERE object_id = $1
          AND object_kind IN ('calendar_event', 'deleted_calendar_event')
        ORDER BY cursor DESC
        LIMIT 2
        "#,
    )
    .bind(fixture.event_id)
    .fetch_all(fixture.storage.pool())
    .await?;
    assert_eq!(changes.len(), 2);
    assert_eq!(
        changes[0].get::<String, _>("object_kind"),
        "deleted_calendar_event"
    );
    assert_eq!(changes[0].get::<String, _>("change_kind"), "created");
    assert_eq!(changes[1].get::<String, _>("object_kind"), "calendar_event");
    assert_eq!(changes[1].get::<String, _>("change_kind"), "destroyed");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT reason FROM tombstones \
             WHERE object_kind = 'calendar_event' AND object_id = $1"
        )
        .bind(fixture.event_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        "move"
    );

    let persisted_mapping = sqlx::query(
        r#"
        SELECT old_mapi_object_id, new_mapi_object_id, old_source_key, new_source_key
        FROM mapi_calendar_event_identity_moves
        WHERE account_id = $1 AND event_id = $2
        "#,
    )
    .bind(fixture.account_id)
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(
        persisted_mapping.get::<i64, _>("old_mapi_object_id"),
        mapi_store_id(50) as i64
    );
    assert_eq!(
        persisted_mapping.get::<i64, _>("new_mapi_object_id"),
        mapi_store_id(101) as i64
    );
    assert_eq!(
        persisted_mapping.get::<Vec<u8>, _>("old_source_key"),
        change_key(50)
    );
    assert_eq!(
        persisted_mapping.get::<Vec<u8>, _>("new_source_key"),
        change_key(101)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mapi_calendar_event_identity_moves WHERE event_id = $1"
        )
        .bind(fixture.event_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        2
    );
    let delegate_mapping = sqlx::query(
        r#"
        SELECT
            old_mapi_object_id,
            new_mapi_object_id,
            old_change_number,
            new_change_number
        FROM mapi_calendar_event_identity_moves
        WHERE account_id = $1 AND event_id = $2
        "#,
    )
    .bind(delegate_account_id)
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(
        delegate_mapping.get::<i64, _>("old_mapi_object_id"),
        mapi_store_id(60) as i64
    );
    assert_eq!(
        delegate_mapping.get::<i64, _>("new_mapi_object_id"),
        mapi_store_id(201) as i64
    );
    assert_eq!(delegate_mapping.get::<i64, _>("old_change_number"), 200);
    assert_eq!(delegate_mapping.get::<i64, _>("new_change_number"), 201);

    assert!(fixture
        .storage
        .update_accessible_event(
            fixture.account_id,
            fixture.event_id,
            updated_event(&fixture, "must not resurrect"),
        )
        .await
        .is_err());

    fixture.cleanup().await
}

#[tokio::test]
async fn event_delete_preserves_custom_shared_calendar_tombstone_scope() -> Result<()> {
    let _guard = database_test_lock().lock().await;
    let Some(fixture) = event_fixture().await? else {
        return Ok(());
    };
    let delegate_account_id = Uuid::new_v4();
    sqlx::query("UPDATE calendars SET role = 'custom' WHERE id = $1")
        .bind(fixture.calendar_id)
        .execute(fixture.storage.pool())
        .await?;
    sqlx::query(
        r#"
        INSERT INTO accounts (
            id, tenant_id, primary_domain_id, primary_email, display_name
        )
        SELECT $1, tenant_id, primary_domain_id, $2, 'Delegate'
        FROM accounts
        WHERE id = $3
        "#,
    )
    .bind(delegate_account_id)
    .bind(format!(
        "delegate-{}@example.test",
        delegate_account_id.simple()
    ))
    .bind(fixture.account_id)
    .execute(fixture.storage.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO calendar_grants (
            id, tenant_id, calendar_id, owner_account_id, grantee_account_id,
            may_read, may_write, may_delete, may_share
        )
        VALUES ($1, $2, $3, $4, $5, TRUE, FALSE, FALSE, FALSE)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(fixture.tenant_id)
    .bind(fixture.calendar_id)
    .bind(fixture.account_id)
    .bind(delegate_account_id)
    .execute(fixture.storage.pool())
    .await?;

    fixture
        .storage
        .delete_client_event(fixture.account_id, fixture.event_id)
        .await?;

    let tombstone = sqlx::query(
        r#"
        SELECT account_id, collection_id, object_uid, reason
        FROM tombstones
        WHERE object_kind = 'calendar_event' AND object_id = $1
        "#,
    )
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(tombstone.get::<Uuid, _>("account_id"), fixture.account_id);
    assert_eq!(
        tombstone.get::<Uuid, _>("collection_id"),
        fixture.calendar_id
    );
    assert_eq!(tombstone.get::<String, _>("object_uid"), "event-uid");
    assert_eq!(tombstone.get::<String, _>("reason"), "move");

    let change = sqlx::query(
        r#"
        SELECT collection_id, affected_principal_ids,
               summary_json->>'collectionId' AS summary_collection_id,
               summary_json->>'objectUid' AS summary_object_uid
        FROM mail_change_log
        WHERE object_kind = 'calendar_event'
          AND object_id = $1
          AND change_kind = 'destroyed'
        "#,
    )
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(change.get::<Uuid, _>("collection_id"), fixture.calendar_id);
    let mut affected_principals = change.get::<Vec<Uuid>, _>("affected_principal_ids");
    affected_principals.sort();
    let mut expected_principals = vec![fixture.account_id, delegate_account_id];
    expected_principals.sort();
    assert_eq!(affected_principals, expected_principals);
    assert_eq!(
        change.get::<String, _>("summary_collection_id"),
        fixture.calendar_id.to_string()
    );
    assert_eq!(change.get::<String, _>("summary_object_uid"), "event-uid");

    let identity = sqlx::query(
        "SELECT object_kind, deleted_at IS NULL AS is_active \
         FROM mapi_object_identities WHERE canonical_id = $1",
    )
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(
        identity.get::<String, _>("object_kind"),
        "deleted_calendar_event"
    );
    assert!(identity.get::<bool, _>("is_active"));
    let lifecycle = sqlx::query(
        "SELECT lifecycle_state, deleted_at IS NOT NULL AS has_deleted_at \
         FROM calendar_events WHERE id = $1",
    )
    .bind(fixture.event_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(lifecycle.get::<String, _>("lifecycle_state"), "deleted");
    assert!(lifecycle.get::<bool, _>("has_deleted_at"));

    fixture.cleanup().await
}

const fn mapi_store_id(global_counter: u64) -> u64 {
    ((global_counter & 0x0000_FFFF_FFFF_FFFF) << 16) | 1
}

fn change_key(global_counter: u64) -> Vec<u8> {
    let mut value = REPLICA_GUID.to_vec();
    let bytes = global_counter.to_be_bytes();
    value.extend_from_slice(&bytes[2..]);
    value
}

fn predecessor_change_list(change_key: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(change_key.len() + 1);
    value.push(change_key.len() as u8);
    value.extend_from_slice(change_key);
    value
}
