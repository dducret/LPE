use std::{env, str::FromStr, sync::OnceLock, time::Duration};

use anyhow::{Context, Result};
use lpe_storage::{
    CanonicalChangeCategory, ContactNameFields, ContactSourceFields, MapiContactCreateInput,
    MapiContactCustomPropertyValue, MapiContactImportDisposition, MapiContactImportObjectDeleted,
    MapiContactImportedIdentity, Storage, UpsertClientContactInput,
};
use serde_json::json;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool, Row,
};
use uuid::Uuid;

const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");
const REPLICA_GUID: [u8; 16] = [
    0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f, 0x10,
];
const IMPORTED_LAST_MODIFICATION_TIME: u64 = 134_128_518_000_000_000;
const TEST_SCHEMA_CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
static DATABASE_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn database_test_lock() -> &'static tokio::sync::Mutex<()> {
    DATABASE_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

struct ContactFixture {
    storage: Storage,
    admin_pool: PgPool,
    schema_name: String,
    tenant_id: Uuid,
    account_id: Uuid,
    schema_cleanup: TestSchemaCleanup,
}

struct TestSchemaCleanup {
    database_url: String,
    schema_name: Option<String>,
}

impl TestSchemaCleanup {
    fn armed(database_url: String, schema_name: String) -> Self {
        Self {
            database_url,
            schema_name: Some(schema_name),
        }
    }

    fn disarm(&mut self) {
        self.schema_name = None;
    }
}

impl Drop for TestSchemaCleanup {
    fn drop(&mut self) {
        let Some(schema_name) = self.schema_name.take() else {
            return;
        };
        let schema_name_for_error = schema_name.clone();
        let database_url = self.database_url.clone();
        let cleanup = std::thread::Builder::new()
            .name("lpe-mapi-contact-schema-cleanup".to_string())
            .spawn(move || -> Result<()> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .context("create temporary-schema cleanup runtime")?;
                runtime.block_on(async move {
                    let pool = PgPoolOptions::new()
                        .max_connections(1)
                        .acquire_timeout(TEST_SCHEMA_CLEANUP_TIMEOUT);
                    let pool = tokio::time::timeout(
                        TEST_SCHEMA_CLEANUP_TIMEOUT,
                        pool.connect_with(PgConnectOptions::from_str(&database_url)?),
                    )
                    .await
                    .context("temporary-schema cleanup connection timed out")?
                    .context("connect for temporary-schema cleanup")?;
                    let result = tokio::time::timeout(
                        TEST_SCHEMA_CLEANUP_TIMEOUT,
                        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
                            .execute(&pool),
                    )
                    .await
                    .context("temporary-schema DROP timed out")?
                    .with_context(|| format!("drop temporary test schema {schema_name}"));
                    drop(pool);
                    result?;
                    Ok(())
                })
            });

        let result = cleanup
            .context("spawn temporary-schema cleanup thread")
            .and_then(|thread| {
                thread
                    .join()
                    .map_err(|_| anyhow::anyhow!("temporary-schema cleanup thread panicked"))?
            });
        if let Err(error) = result {
            eprintln!("unable to clean temporary schema {schema_name_for_error}: {error:#}");
        }
    }
}

impl ContactFixture {
    async fn cleanup(mut self) -> Result<()> {
        self.storage.pool().close().await;
        sqlx::query(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            self.schema_name
        ))
        .execute(&self.admin_pool)
        .await?;
        self.schema_cleanup.disarm();
        self.admin_pool.close().await;
        Ok(())
    }
}

async fn contact_fixture() -> Result<Option<ContactFixture>> {
    let Some(database_url) = env::var("TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!(
            "skipping PostgreSQL-backed MAPI Contact create test; TEST_DATABASE_URL is not set"
        );
        return Ok(None);
    };

    let schema_name = format!("lpe_mapi_contact_{}", Uuid::new_v4().simple());
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
    let schema_cleanup = TestSchemaCleanup::armed(database_url.clone(), schema_name.clone());

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
    sqlx::query("INSERT INTO tenants (id, slug, display_name) VALUES ($1, $2, 'MAPI Contact')")
        .bind(tenant_id)
        .bind(format!("mapi-contact-{}", tenant_id.simple()))
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
         VALUES ($1, $2, $3, $4, 'Alice Élodie')",
    )
    .bind(account_id)
    .bind(tenant_id)
    .bind(domain_id)
    .bind(format!("alice@{}.example.test", tenant_id.simple()))
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO mapi_mailbox_replicas \
         (tenant_id, account_id, replica_guid, next_global_counter) VALUES ($1, $2, $3, 1000)",
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(Uuid::from_bytes(REPLICA_GUID))
    .execute(&pool)
    .await?;

    Ok(Some(ContactFixture {
        storage: Storage::new(pool),
        admin_pool,
        schema_name,
        tenant_id,
        account_id,
        schema_cleanup,
    }))
}

fn imported_identity(global_counter: u64) -> MapiContactImportedIdentity {
    let mut change_key = Uuid::from_u128(0x6745_4820_6960_ca40_9d80_0817060fa2c1)
        .as_bytes()
        .to_vec();
    change_key.extend_from_slice(&0x0457u32.to_be_bytes());
    let mut predecessor_change_list = vec![change_key.len() as u8];
    predecessor_change_list.extend_from_slice(&change_key);
    MapiContactImportedIdentity {
        source_key: source_key(global_counter),
        change_key,
        predecessor_change_list,
        last_modification_time: IMPORTED_LAST_MODIFICATION_TIME,
    }
}

fn contact_input(account_id: Uuid, contact_id: Uuid, name: &str) -> UpsertClientContactInput {
    UpsertClientContactInput {
        id: Some(contact_id),
        account_id,
        name: name.to_string(),
        role: "Architecte".to_string(),
        email: "rene.maguaretaz@example.test".to_string(),
        phone: "+41 22 555 01 02".to_string(),
        team: "Interopérabilité".to_string(),
        notes: "Créé par Outlook en MAPI over HTTP".to_string(),
        structured_name: ContactNameFields {
            prefix: "Dr".to_string(),
            given: "René".to_string(),
            middle: "Émile".to_string(),
            family: "Maguaretaz".to_string(),
            suffix: "III".to_string(),
            nickname: "Rémi".to_string(),
            phonetic_given: "Rene".to_string(),
            phonetic_family: "Maguaretaz".to_string(),
        },
        emails_json: Some(json!([
            {"email": "rene.maguaretaz@example.test", "label": "work", "isDefault": true},
            {"email": "rene@example.net", "label": "home"}
        ])),
        phones_json: Some(json!([
            {"phone": "+41 22 555 01 02", "label": "work"},
            {"phone": "+41 79 555 01 03", "label": "mobile"}
        ])),
        addresses_json: Some(json!([
            {"label": "work", "street": "1 rue du Rhône", "locality": "Genève", "country": "CH"},
            {"label": "home", "street": "2 chemin de l'Été", "locality": "Carouge", "country": "CH"}
        ])),
        urls_json: Some(json!([{"label": "work", "url": "https://example.test/rené"}])),
        organization_name: "LPE Genève".to_string(),
        job_title: "Architecte protocoles".to_string(),
        raw_vcard: Some(
            "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:René Maguaretaz\r\nEND:VCARD\r\n".to_string(),
        ),
        raw_vcard_is_explicit: true,
        source: ContactSourceFields {
            import_source: "mapi".to_string(),
            source_uid: Some(format!("outlook-{contact_id}")),
            source_etag: Some("W/\"contact-1\"".to_string()),
            source_payload_json: json!({"categories": ["Équipe", "Genève"]}),
        },
        source_is_explicit: true,
    }
}

fn create_input(
    account_id: Uuid,
    contact_id: Uuid,
    identity: Option<MapiContactImportedIdentity>,
) -> MapiContactCreateInput {
    MapiContactCreateInput {
        principal_account_id: account_id,
        collection_id: "default".to_string(),
        mapi_folder_id: mapi_store_id(15),
        contact: contact_input(account_id, contact_id, "René Maguaretaz"),
        imported_identity: identity,
        fail_on_conflict: false,
        custom_property_upserts: vec![MapiContactCustomPropertyValue {
            property_tag: 0x8001_001F,
            property_type: 0x001F,
            property_value: "Catégorie Outlook".as_bytes().to_vec(),
        }],
    }
}

#[tokio::test]
async fn mapi_contact_create_is_atomic_and_preserves_reserved_import_identity() -> Result<()> {
    let _guard = database_test_lock().lock().await;
    let Some(fixture) = contact_fixture().await? else {
        return Ok(());
    };

    let rejected_contact_id = Uuid::new_v4();
    let error = fixture
        .storage
        .create_mapi_contact(create_input(
            fixture.account_id,
            rejected_contact_id,
            Some(imported_identity(700)),
        ))
        .await
        .expect_err("an unreserved client MID must reject the entire create");
    assert!(error.to_string().contains("not locally reserved"));

    for table in [
        "contacts",
        "contact_books",
        "mapi_object_identities",
        "mapi_custom_property_values",
        "mail_change_log",
        "canonical_change_journal",
        "account_sync_state",
    ] {
        let count = sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(fixture.storage.pool())
            .await?;
        assert_eq!(count, 0, "{table} retained partial state after rejection");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT next_global_counter FROM mapi_mailbox_replicas WHERE account_id = $1"
        )
        .bind(fixture.account_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        1000
    );

    sqlx::query(
        r#"
        INSERT INTO mapi_local_replica_id_ranges (
            tenant_id, account_id, replica_guid,
            first_global_counter, end_global_counter_exclusive
        )
        VALUES ($1, $2, $3, 500, 501)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(Uuid::from_bytes(REPLICA_GUID))
    .execute(fixture.storage.pool())
    .await?;

    sqlx::query(
        r#"
        INSERT INTO mapi_special_folder_aliases (
            tenant_id, account_id, alias_folder_id, canonical_folder_id,
            source_key, mapi_change_number
        )
        VALUES ($1, $2, $3, $4, $5, 999)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(mapi_store_id(500) as i64)
    .bind(mapi_store_id(15) as i64)
    .bind(source_key(500))
    .execute(fixture.storage.pool())
    .await?;
    let alias_error = fixture
        .storage
        .create_mapi_contact(create_input(
            fixture.account_id,
            Uuid::new_v4(),
            Some(imported_identity(500)),
        ))
        .await
        .expect_err("a special-folder alias must own its SourceKey and MID");
    assert!(alias_error.to_string().contains("special-folder alias"));
    sqlx::query("DELETE FROM mapi_special_folder_aliases WHERE tenant_id = $1 AND account_id = $2")
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .execute(fixture.storage.pool())
        .await?;

    sqlx::query(
        r#"
        INSERT INTO mapi_local_replica_deleted_ranges (
            tenant_id, account_id, folder_id, replica_guid,
            min_global_counter, max_global_counter
        )
        VALUES ($1, $2, $3, $4, 500, 500)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(mapi_store_id(15) as i64)
    .bind(Uuid::from_bytes(REPLICA_GUID))
    .execute(fixture.storage.pool())
    .await?;
    let deleted_error = fixture
        .storage
        .create_mapi_contact(create_input(
            fixture.account_id,
            Uuid::new_v4(),
            Some(imported_identity(500)),
        ))
        .await
        .expect_err("a SourceKey deleted from Contacts must not be reused");
    assert!(deleted_error.is::<MapiContactImportObjectDeleted>());
    sqlx::query(
        "UPDATE mapi_local_replica_deleted_ranges SET folder_id = $3 \
         WHERE tenant_id = $1 AND account_id = $2",
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(mapi_store_id(16) as i64)
    .execute(fixture.storage.pool())
    .await?;

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM contacts")
            .fetch_one(fixture.storage.pool())
            .await?,
        0,
        "rejected alias/deleted-range imports retained a canonical Contact"
    );

    let mut listener = fixture
        .storage
        .create_canonical_change_listener(fixture.account_id)
        .await?;
    let imported_contact_id = Uuid::new_v4();
    let identity = imported_identity(500);
    let created = fixture
        .storage
        .create_mapi_contact(create_input(
            fixture.account_id,
            imported_contact_id,
            Some(identity.clone()),
        ))
        .await?;

    assert_eq!(created.contact.id, imported_contact_id);
    assert_eq!(created.contact.collection_id, "default");
    assert_eq!(created.contact.name, "René Maguaretaz");
    assert_eq!(created.contact.emails_json.as_array().unwrap().len(), 2);
    assert_eq!(created.contact.phones_json.as_array().unwrap().len(), 2);
    assert_eq!(created.contact.addresses_json.as_array().unwrap().len(), 2);
    assert_eq!(created.mapi_object_id, mapi_store_id(500));
    assert_eq!(created.version.change_number, 1000);
    assert_ne!(created.version.change_number, 500);
    assert_eq!(created.version.change_key, identity.change_key);
    assert_eq!(
        created.version.predecessor_change_list,
        identity.predecessor_change_list
    );
    assert_eq!(
        created.version.last_modification_time,
        IMPORTED_LAST_MODIFICATION_TIME
    );

    let persisted = sqlx::query(
        r#"
        SELECT
            contact.import_source,
            contact.modseq,
            book.sync_modseq,
            identity.account_id AS identity_account_id,
            identity.mapi_global_counter,
            identity.mapi_object_id,
            identity.source_key,
            identity.change_key,
            identity.predecessor_change_list,
            identity.mapi_change_number,
            (EXTRACT(EPOCH FROM (
                identity.updated_at - TIMESTAMPTZ '1601-01-01 00:00:00+00'
            )) * 10000000)::bigint AS identity_filetime,
            (EXTRACT(EPOCH FROM (
                contact.updated_at - TIMESTAMPTZ '1601-01-01 00:00:00+00'
            )) * 10000000)::bigint AS contact_filetime
        FROM contacts contact
        JOIN contact_books book
          ON book.tenant_id = contact.tenant_id
         AND book.id = contact.contact_book_id
        JOIN mapi_object_identities identity
          ON identity.tenant_id = contact.tenant_id
         AND identity.canonical_id = contact.id
         AND identity.object_kind = 'contact'
        WHERE contact.id = $1
        "#,
    )
    .bind(imported_contact_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(persisted.get::<String, _>("import_source"), "mapi");
    assert_eq!(persisted.get::<i64, _>("modseq"), 2);
    assert_eq!(persisted.get::<i64, _>("sync_modseq"), 2);
    assert_eq!(
        persisted.get::<Uuid, _>("identity_account_id"),
        fixture.account_id
    );
    assert_eq!(persisted.get::<i64, _>("mapi_global_counter"), 500);
    assert_eq!(
        persisted.get::<i64, _>("mapi_object_id"),
        mapi_store_id(500) as i64
    );
    assert_eq!(persisted.get::<Vec<u8>, _>("source_key"), source_key(500));
    assert_eq!(
        persisted.get::<Vec<u8>, _>("change_key"),
        identity.change_key
    );
    assert_eq!(
        persisted.get::<Vec<u8>, _>("predecessor_change_list"),
        identity.predecessor_change_list
    );
    assert_eq!(persisted.get::<i64, _>("mapi_change_number"), 1000);
    assert_eq!(
        persisted.get::<i64, _>("identity_filetime") as u64,
        IMPORTED_LAST_MODIFICATION_TIME
    );
    assert_eq!(
        persisted.get::<i64, _>("contact_filetime") as u64,
        IMPORTED_LAST_MODIFICATION_TIME
    );

    let custom_property = sqlx::query(
        "SELECT account_id, property_type, property_value \
         FROM mapi_custom_property_values \
         WHERE object_kind = 'contact' AND canonical_id = $1 AND property_tag = $2",
    )
    .bind(imported_contact_id)
    .bind(i64::from(0x8001_001Fu32))
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(
        custom_property.get::<Uuid, _>("account_id"),
        fixture.account_id
    );
    assert_eq!(custom_property.get::<i32, _>("property_type"), 0x001F);
    assert_eq!(
        custom_property.get::<Vec<u8>, _>("property_value"),
        "Catégorie Outlook".as_bytes()
    );

    let change = sqlx::query(
        "SELECT change_kind, affected_principal_ids, summary_json \
         FROM mail_change_log WHERE object_kind = 'contact' AND object_id = $1",
    )
    .bind(imported_contact_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    assert_eq!(change.get::<String, _>("change_kind"), "created");
    assert_eq!(
        change.get::<Vec<Uuid>, _>("affected_principal_ids"),
        vec![fixture.account_id]
    );
    assert_eq!(
        change.get::<serde_json::Value, _>("summary_json")["mapiChangeNumber"],
        json!(1000)
    );

    let pushed = tokio::time::timeout(
        Duration::from_secs(5),
        listener.wait_for_change(&[CanonicalChangeCategory::Contacts]),
    )
    .await
    .context("Contacts notification timed out")??;
    assert_eq!(
        pushed.accounts_for(CanonicalChangeCategory::Contacts),
        [fixture.account_id].into_iter().collect()
    );

    let visible = fixture
        .storage
        .fetch_accessible_contacts_in_collection(fixture.account_id, "default")
        .await?;
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, imported_contact_id);

    let change_rows_before_retry = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM mail_change_log WHERE object_kind = 'contact' AND object_id = $1",
    )
    .bind(imported_contact_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    let next_counter_before_retry = sqlx::query_scalar::<_, i64>(
        "SELECT next_global_counter FROM mapi_mailbox_replicas WHERE account_id = $1",
    )
    .bind(fixture.account_id)
    .fetch_one(fixture.storage.pool())
    .await?;
    let replay = fixture
        .storage
        .create_mapi_contact(create_input(
            fixture.account_id,
            Uuid::new_v4(),
            Some(identity.clone()),
        ))
        .await?;
    assert_eq!(replay.contact.id, imported_contact_id);
    assert_eq!(replay.mapi_object_id, mapi_store_id(500));
    assert_eq!(replay.version.change_number, 1000);
    assert_eq!(
        replay.import_disposition,
        MapiContactImportDisposition::IgnoredOlderOrSame
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM contacts")
            .fetch_one(fixture.storage.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mail_change_log WHERE object_kind = 'contact' AND object_id = $1",
        )
        .bind(imported_contact_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        change_rows_before_retry
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT next_global_counter FROM mapi_mailbox_replicas WHERE account_id = $1",
        )
        .bind(fixture.account_id)
        .fetch_one(fixture.storage.pool())
        .await?,
        next_counter_before_retry
    );

    let mut successor_identity = identity.clone();
    *successor_identity.change_key.last_mut().unwrap() += 1;
    successor_identity.predecessor_change_list =
        predecessor_change_list(&successor_identity.change_key);
    successor_identity.last_modification_time += 600_000_000;
    let mut successor_input = create_input(
        fixture.account_id,
        Uuid::new_v4(),
        Some(successor_identity.clone()),
    );
    successor_input.contact.name = "René Maguaretaz modifié".to_string();
    successor_input.contact.email = "rene.updated@example.test".to_string();
    successor_input.contact.emails_json = Some(json!([{
        "email": "rene.updated@example.test",
        "label": "work",
        "isDefault": true
    }]));
    let updated = fixture.storage.create_mapi_contact(successor_input).await?;
    assert_eq!(updated.contact.id, imported_contact_id);
    assert_eq!(updated.mapi_object_id, mapi_store_id(500));
    assert_eq!(updated.contact.name, "René Maguaretaz modifié");
    assert_eq!(updated.contact.email, "rene.updated@example.test");
    assert_eq!(updated.version.change_number, 1001);
    assert_eq!(updated.version.change_key, successor_identity.change_key);
    assert_eq!(
        updated.version.predecessor_change_list,
        successor_identity.predecessor_change_list
    );
    assert_eq!(
        updated.import_disposition,
        MapiContactImportDisposition::Applied
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM contacts")
            .fetch_one(fixture.storage.pool())
            .await?,
        1
    );

    let successor_replay = fixture
        .storage
        .create_mapi_contact(create_input(
            fixture.account_id,
            Uuid::new_v4(),
            Some(successor_identity),
        ))
        .await?;
    assert_eq!(successor_replay.contact.id, imported_contact_id);
    assert_eq!(successor_replay.version.change_number, 1001);
    assert_eq!(
        successor_replay.import_disposition,
        MapiContactImportDisposition::IgnoredOlderOrSame
    );

    let online_contact_id = Uuid::new_v4();
    let online = fixture
        .storage
        .create_mapi_contact(create_input(fixture.account_id, online_contact_id, None))
        .await?;
    assert_eq!(online.version.change_number, 1002);
    assert_eq!(online.mapi_object_id, mapi_store_id(1002));
    assert_eq!(online.version.change_key, source_key(1002));
    assert_eq!(
        online.version.predecessor_change_list,
        predecessor_change_list(&source_key(1002))
    );
    assert!(online.version.last_modification_time > IMPORTED_LAST_MODIFICATION_TIME);
    assert_eq!(online.version.last_modification_time % 10, 0);

    fixture.cleanup().await
}

const fn mapi_store_id(global_counter: u64) -> u64 {
    ((global_counter & 0x0000_FFFF_FFFF_FFFF) << 16) | 1
}

fn source_key(global_counter: u64) -> Vec<u8> {
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
