use std::collections::{BTreeMap, HashSet};

use anyhow::{anyhow, bail, Result};
use serde_json::Value;
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::mapi_events::{
    mapi_change_key, mapi_store_id, merge_predecessor_change_list,
    FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER, FIRST_RESERVED_HIGH_GLOBAL_COUNTER, MAPI_STORE_REPLICA_GUID,
    MAX_MAPI_GLOBAL_COUNTER,
};
use crate::workspace::{
    contact_array_json, contact_emails_json, contact_phones_json, contact_primary_email,
    contact_source_payload_json,
};
use crate::{
    AccessibleContact, CanonicalChangeCategory, ContactNameFields, ContactSourceFields, Storage,
    UpsertClientContactInput, DEFAULT_CONTACT_BOOK_ROLE, IM_CONTACT_LIST_COLLECTION_ID,
    IM_CONTACT_LIST_ROLE, QUICK_CONTACTS_COLLECTION_ID, QUICK_CONTACTS_ROLE,
    SUGGESTED_CONTACTS_COLLECTION_ID, SUGGESTED_CONTACTS_ROLE,
};

#[derive(Debug, Clone)]
pub struct MapiContactCreateInput {
    pub principal_account_id: Uuid,
    pub collection_id: String,
    pub mapi_folder_id: u64,
    pub contact: UpsertClientContactInput,
    pub imported_identity: Option<MapiContactImportedIdentity>,
    pub fail_on_conflict: bool,
    pub custom_property_upserts: Vec<MapiContactCustomPropertyValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapiContactImportedIdentity {
    pub source_key: Vec<u8>,
    pub change_key: Vec<u8>,
    pub predecessor_change_list: Vec<u8>,
    pub last_modification_time: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapiContactCustomPropertyValue {
    pub property_tag: u32,
    pub property_type: u16,
    pub property_value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapiContactVersion {
    /// The 48-bit GLOBCNT. The Exchange adapter projects the wire CN with ReplId 1.
    pub change_number: u64,
    pub change_key: Vec<u8>,
    pub predecessor_change_list: Vec<u8>,
    pub last_modification_time: u64,
}

#[derive(Debug, Clone)]
pub struct MapiContactCreateResult {
    pub contact: AccessibleContact,
    pub mapi_object_id: u64,
    pub version: MapiContactVersion,
    pub import_disposition: MapiContactImportDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapiContactImportDisposition {
    Applied,
    IgnoredOlderOrSame,
    ConflictResolved { imported_wins: bool },
}

impl MapiContactImportDisposition {
    pub fn changes_server_replica(self) -> bool {
        !matches!(self, Self::IgnoredOlderOrSame)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapiContactImportConflict;

impl std::fmt::Display for MapiContactImportConflict {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MAPI Contact import conflict")
    }
}

impl std::error::Error for MapiContactImportConflict {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapiContactImportObjectDeleted;

impl std::fmt::Display for MapiContactImportObjectDeleted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MAPI Contact import object deleted")
    }
}

impl std::error::Error for MapiContactImportObjectDeleted {}

struct AllocatedContactIdentity {
    object_id: u64,
    change_number: u64,
    change_key: Vec<u8>,
    predecessor_change_list: Vec<u8>,
}

struct ExistingContactIdentityCommit {
    canonical_id: Uuid,
    identity: AllocatedContactIdentity,
    last_modification_time: u64,
    apply_imported_content: bool,
    disposition: MapiContactImportDisposition,
}

impl Storage {
    pub async fn create_mapi_contact(
        &self,
        input: MapiContactCreateInput,
    ) -> Result<MapiContactCreateResult> {
        let normalized = NormalizedContact::from_input(&input.contact)?;
        if input.mapi_folder_id == 0 || input.mapi_folder_id > i64::MAX as u64 {
            bail!("MAPI Contact folder id is outside the PostgreSQL identifier range");
        }
        validate_custom_properties(&input.custom_property_upserts)?;
        if let Some(identity) = input.imported_identity.as_ref() {
            validate_imported_identity(identity)?;
        }

        let collection_id = input.collection_id.trim();
        let collection = self
            .fetch_accessible_contact_collections(input.principal_account_id)
            .await?
            .into_iter()
            .find(|collection| collection.id == collection_id)
            .ok_or_else(|| anyhow!("contact collection is not accessible"))?;
        if !collection.rights.may_write {
            bail!("write access is not granted on this address book");
        }

        let tenant_id = self
            .tenant_id_for_account_id(input.principal_account_id)
            .await?;
        let owner_account_id = collection.owner_account_id;
        let requested_contact_id = input.contact.id.unwrap_or_else(Uuid::new_v4);
        let role = contact_book_role(collection_id);
        let mut tx = self.pool().begin().await?;
        let contact_book_id =
            Self::ensure_contact_book_in_tx(&mut tx, &tenant_id, owner_account_id, role).await?;
        recheck_contact_write_access_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            contact_book_id,
            input.principal_account_id,
        )
        .await?;

        if let Some(imported_identity) = input.imported_identity.as_ref() {
            if let Some(committed) = commit_existing_contact_import_in_tx(
                &mut tx,
                &tenant_id,
                input.principal_account_id,
                owner_account_id,
                contact_book_id,
                imported_identity,
                input.fail_on_conflict,
            )
            .await?
            {
                let contact_id = committed.canonical_id;
                if committed.apply_imported_content {
                    update_contact_in_tx(
                        &mut tx,
                        &tenant_id,
                        owner_account_id,
                        contact_book_id,
                        contact_id,
                        &normalized,
                        committed.last_modification_time,
                    )
                    .await?;
                    upsert_custom_properties_in_tx(
                        &mut tx,
                        &tenant_id,
                        input.principal_account_id,
                        contact_id,
                        &input.custom_property_upserts,
                    )
                    .await?;
                }
                if committed.disposition.changes_server_replica() {
                    record_contact_change_in_tx(
                        self,
                        &mut tx,
                        &tenant_id,
                        owner_account_id,
                        contact_book_id,
                        contact_id,
                        "updated",
                        false,
                        !input.custom_property_upserts.is_empty(),
                        committed.identity.change_number,
                    )
                    .await?;
                }
                let version = MapiContactVersion {
                    change_number: committed.identity.change_number,
                    change_key: committed.identity.change_key,
                    predecessor_change_list: committed.identity.predecessor_change_list,
                    last_modification_time: committed.last_modification_time,
                };
                let mapi_object_id = committed.identity.object_id;
                let import_disposition = committed.disposition;
                tx.commit().await?;
                let contact = self
                    .fetch_accessible_contacts_by_ids(input.principal_account_id, &[contact_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("MAPI Contact not visible after imported commit"))?;
                return Ok(MapiContactCreateResult {
                    contact,
                    mapi_object_id,
                    version,
                    import_disposition,
                });
            }
        }

        let last_modification_time = match input.imported_identity.as_ref() {
            Some(identity) => normalize_filetime(identity.last_modification_time)?,
            None => current_filetime_in_tx(&mut tx).await?,
        };

        let identity = allocate_contact_identity_in_tx(
            &mut tx,
            &tenant_id,
            input.principal_account_id,
            input.mapi_folder_id,
            requested_contact_id,
            input.imported_identity.as_ref(),
            last_modification_time,
        )
        .await?;

        let contact_id = requested_contact_id;

        insert_contact_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            contact_book_id,
            contact_id,
            &normalized,
            last_modification_time,
        )
        .await?;
        insert_custom_properties_in_tx(
            &mut tx,
            &tenant_id,
            input.principal_account_id,
            contact_id,
            &input.custom_property_upserts,
        )
        .await?;

        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                owner_account_id,
                CanonicalChangeCategory::Contacts.as_str(),
            )
            .await?;
        set_created_contact_modseq_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            contact_book_id,
            contact_id,
            modseq,
        )
        .await?;
        let affected_principals = contact_affected_principals_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            contact_book_id,
        )
        .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(owner_account_id),
            None,
            "contact",
            contact_id,
            "created",
            modseq,
            &affected_principals,
            serde_json::json!({
                "collectionId": contact_book_id,
                "objectUid": contact_id.to_string(),
                "created": true,
                "customPropertiesChanged": !input.custom_property_upserts.is_empty(),
                "mapiChangeNumber": identity.change_number,
            }),
        )
        .await?;
        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Contacts,
            owner_account_id,
        )
        .await?;

        let contact = AccessibleContact {
            id: contact_id,
            collection_id: collection.id,
            owner_account_id,
            owner_email: collection.owner_email,
            owner_display_name: collection.owner_display_name,
            rights: collection.rights,
            name: normalized.name,
            role: normalized.role,
            email: normalized.email,
            phone: normalized.phone,
            team: normalized.team,
            notes: normalized.notes,
            structured_name: normalized.structured_name,
            emails_json: normalized.emails_json,
            phones_json: normalized.phones_json,
            addresses_json: normalized.addresses_json,
            urls_json: normalized.urls_json,
            organization_name: normalized.organization_name,
            job_title: normalized.job_title,
            raw_vcard: normalized.raw_vcard,
            source: ContactSourceFields {
                import_source: "mapi".to_string(),
                source_uid: normalized.source_uid,
                source_etag: normalized.source_etag,
                source_payload_json: normalized.source_payload_json,
            },
        };
        let version = MapiContactVersion {
            change_number: identity.change_number,
            change_key: identity.change_key,
            predecessor_change_list: identity.predecessor_change_list,
            last_modification_time,
        };
        let mapi_object_id = identity.object_id;
        tx.commit().await?;

        Ok(MapiContactCreateResult {
            contact,
            mapi_object_id,
            version,
            import_disposition: MapiContactImportDisposition::Applied,
        })
    }
}

struct NormalizedContact {
    name: String,
    role: String,
    email: String,
    phone: String,
    team: String,
    notes: String,
    structured_name: ContactNameFields,
    emails_json: Value,
    phones_json: Value,
    addresses_json: Value,
    urls_json: Value,
    organization_name: String,
    job_title: String,
    raw_vcard: Option<String>,
    source_uid: Option<String>,
    source_etag: Option<String>,
    source_payload_json: Value,
}

impl NormalizedContact {
    fn from_input(input: &UpsertClientContactInput) -> Result<Self> {
        let name = input.name.trim().to_string();
        let emails_json = contact_emails_json(input)?;
        let email = contact_primary_email(&emails_json);
        if name.is_empty() || email.is_empty() {
            bail!("contact name and email are required");
        }
        let phones_json = contact_phones_json(input)?;
        let phone = phones_json
            .as_array()
            .and_then(|items| items.first())
            .and_then(|item| item.get("phone"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string();
        let addresses_json = contact_array_json(input.addresses_json.clone())?;
        let urls_json = contact_array_json(input.urls_json.clone())?;
        let source_payload_json =
            contact_source_payload_json(input.source.source_payload_json.clone())?;
        let team = input.team.trim().to_string();
        let organization_name = if input.organization_name.trim().is_empty() {
            team.clone()
        } else {
            input.organization_name.trim().to_string()
        };
        Ok(Self {
            name,
            role: input.role.trim().to_string(),
            email,
            phone,
            team,
            notes: input.notes.trim().to_string(),
            structured_name: ContactNameFields {
                prefix: input.structured_name.prefix.trim().to_string(),
                given: input.structured_name.given.trim().to_string(),
                middle: input.structured_name.middle.trim().to_string(),
                family: input.structured_name.family.trim().to_string(),
                suffix: input.structured_name.suffix.trim().to_string(),
                nickname: input.structured_name.nickname.trim().to_string(),
                phonetic_given: input.structured_name.phonetic_given.trim().to_string(),
                phonetic_family: input.structured_name.phonetic_family.trim().to_string(),
            },
            emails_json,
            phones_json,
            addresses_json,
            urls_json,
            organization_name,
            job_title: input.job_title.trim().to_string(),
            raw_vcard: input.raw_vcard.clone(),
            source_uid: input.source.source_uid.clone(),
            source_etag: input.source.source_etag.clone(),
            source_payload_json,
        })
    }
}

fn contact_book_role(collection_id: &str) -> &'static str {
    match collection_id.trim() {
        SUGGESTED_CONTACTS_COLLECTION_ID => SUGGESTED_CONTACTS_ROLE,
        QUICK_CONTACTS_COLLECTION_ID => QUICK_CONTACTS_ROLE,
        IM_CONTACT_LIST_COLLECTION_ID => IM_CONTACT_LIST_ROLE,
        _ => DEFAULT_CONTACT_BOOK_ROLE,
    }
}

fn validate_custom_properties(values: &[MapiContactCustomPropertyValue]) -> Result<()> {
    let mut tags = HashSet::new();
    for value in values {
        if value.property_type != (value.property_tag & 0xFFFF) as u16 {
            bail!("MAPI custom property type does not match its property tag");
        }
        if !tags.insert(value.property_tag) {
            bail!("MAPI custom property upserts contain a duplicate property tag");
        }
    }
    Ok(())
}

fn validate_imported_identity(identity: &MapiContactImportedIdentity) -> Result<()> {
    if identity.source_key.len() != 22 {
        bail!("MAPI Contact imported SourceKey must be exactly 22 bytes");
    }
    if !(17..=24).contains(&identity.change_key.len()) {
        bail!("MAPI Contact imported ChangeKey XID must be between 17 and 24 bytes");
    }
    // [MS-OXCFXICS] section 3.3.5.2.3: an uploaded foreign XID cannot
    // have the six-byte LocalId length used by a server GID.
    if identity.change_key.len() == 22 {
        bail!("MAPI Contact imported ChangeKey must be a foreign XID");
    }
    let normalized =
        merge_predecessor_change_list(&identity.predecessor_change_list, &identity.change_key)
            .map_err(|_| {
                anyhow!("MAPI Contact imported PCL must be canonical and contain its ChangeKey")
            })?;
    if normalized != identity.predecessor_change_list {
        bail!("MAPI Contact imported PCL must be canonical and contain its ChangeKey");
    }
    normalize_filetime(identity.last_modification_time)?;
    Ok(())
}

fn normalize_filetime(value: u64) -> Result<u64> {
    if value == 0 || value > i64::MAX as u64 {
        bail!("MAPI Contact LastModificationTime is outside the PostgreSQL FILETIME range");
    }
    Ok(value - value % 10)
}

async fn current_filetime_in_tx(tx: &mut sqlx::Transaction<'_, Postgres>) -> Result<u64> {
    let value = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT (
            EXTRACT(EPOCH FROM (
                clock_timestamp() - TIMESTAMPTZ '1601-01-01 00:00:00+00'
            )) * 1000000
        )::bigint * 10
        "#,
    )
    .fetch_one(&mut **tx)
    .await?;
    if value <= 0 {
        bail!("PostgreSQL returned an invalid current MAPI FILETIME");
    }
    Ok(value as u64)
}

async fn recheck_contact_write_access_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    contact_book_id: Uuid,
    principal_account_id: Uuid,
) -> Result<()> {
    if owner_account_id == principal_account_id {
        sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            FOR SHARE
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .fetch_one(&mut **tx)
        .await?;
        return Ok(());
    }
    let may_write = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT may_write
        FROM contact_book_grants
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND contact_book_id = $3
          AND grantee_account_id = $4
        FOR SHARE
        "#,
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(contact_book_id)
    .bind(principal_account_id)
    .fetch_optional(&mut **tx)
    .await?
    .unwrap_or(false);
    if !may_write {
        bail!("write access is not granted on this address book");
    }
    Ok(())
}

async fn commit_existing_contact_import_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    principal_account_id: Uuid,
    owner_account_id: Uuid,
    contact_book_id: Uuid,
    imported: &MapiContactImportedIdentity,
    fail_on_conflict: bool,
) -> Result<Option<ExistingContactIdentityCommit>> {
    let replica_guid = lock_contact_replica_in_tx(tx, tenant_id, principal_account_id).await?;
    let source_counter = imported_source_counter(imported, replica_guid)?;
    let object_id = mapi_store_id(source_counter);
    let existing = sqlx::query(
        r#"
        SELECT
            identity.object_kind,
            identity.canonical_id,
            identity.mapi_object_id,
            identity.mapi_change_number,
            identity.source_key,
            identity.change_key,
            identity.predecessor_change_list,
            identity.deleted_at IS NOT NULL AS was_deleted,
            (EXTRACT(EPOCH FROM (
                identity.updated_at - TIMESTAMPTZ '1601-01-01 00:00:00+00'
            )) * 10000000)::bigint AS last_modification_time,
            contact.owner_account_id,
            contact.contact_book_id
        FROM mapi_object_identities identity
        LEFT JOIN contacts contact
          ON contact.tenant_id = identity.tenant_id
         AND contact.id = identity.canonical_id
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND (identity.mapi_object_id = $3 OR identity.source_key = $4)
        LIMIT 1
        FOR UPDATE OF identity
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(object_id as i64)
    .bind(&imported.source_key)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = existing else {
        return Ok(None);
    };
    if row.get::<String, _>("object_kind") != "contact"
        || row.get::<i64, _>("mapi_object_id") as u64 != object_id
        || row.get::<Vec<u8>, _>("source_key") != imported.source_key
    {
        bail!("imported Contact identity collides with another MAPI object");
    }
    if row.get::<bool, _>("was_deleted") {
        // [MS-OXCFXICS] section 3.3.4.3.3.2.2.1 allows ecObjectDeleted
        // from SaveChangesMessage and forbids resurrection of the SourceKey.
        return Err(MapiContactImportObjectDeleted.into());
    }
    if row.get::<Option<Uuid>, _>("owner_account_id") != Some(owner_account_id)
        || row.get::<Option<Uuid>, _>("contact_book_id") != Some(contact_book_id)
    {
        bail!("imported Contact identity is outside the target address book");
    }

    let canonical_id = row.get::<Uuid, _>("canonical_id");
    let current_change_number = row.get::<i64, _>("mapi_change_number");
    if current_change_number <= 0 || current_change_number as u64 > MAX_MAPI_GLOBAL_COUNTER {
        bail!("stored MAPI Contact change number is outside the GLOBCNT range");
    }
    let current_change_key = row.get::<Vec<u8>, _>("change_key");
    let current_predecessor_change_list = row.get::<Vec<u8>, _>("predecessor_change_list");
    let current_last_modification_time = row.get::<i64, _>("last_modification_time");
    if current_last_modification_time <= 0 {
        bail!("stored MAPI Contact LastModificationTime is invalid");
    }
    let current_entries = parse_contact_predecessor_change_list(&current_predecessor_change_list)?;
    let imported_entries =
        parse_contact_predecessor_change_list(&imported.predecessor_change_list)?;
    if contact_predecessors_include(&current_entries, &imported_entries)?
        && contact_predecessors_contain_change_key(&current_entries, &imported.change_key)?
    {
        return Ok(Some(ExistingContactIdentityCommit {
            canonical_id,
            identity: AllocatedContactIdentity {
                object_id,
                change_number: current_change_number as u64,
                change_key: current_change_key,
                predecessor_change_list: current_predecessor_change_list,
            },
            last_modification_time: current_last_modification_time as u64,
            apply_imported_content: false,
            disposition: MapiContactImportDisposition::IgnoredOlderOrSame,
        }));
    }

    let conflict = !contact_predecessors_include(&imported_entries, &current_entries)?;
    if conflict && fail_on_conflict {
        return Err(MapiContactImportConflict.into());
    }
    let change_number = allocate_next_contact_change_number_in_tx(
        tx,
        tenant_id,
        principal_account_id,
        source_counter.saturating_add(1),
    )
    .await?;
    let predecessor_change_list =
        merge_contact_predecessor_change_lists(current_entries, imported_entries)?;
    let imported_last_modification_time = normalize_filetime(imported.last_modification_time)?;
    let imported_wins = !conflict
        || imported_contact_version_wins_last_writer(
            imported_last_modification_time,
            &imported.change_key,
            current_last_modification_time as u64,
            &current_change_key,
        )?;
    let (change_key, last_modification_time) = if imported_wins {
        (imported.change_key.clone(), imported_last_modification_time)
    } else {
        (current_change_key, current_last_modification_time as u64)
    };
    sqlx::query(
        r#"
        UPDATE mapi_object_identities
        SET mapi_change_number = $5,
            change_key = $6,
            predecessor_change_list = $7,
            updated_at = TIMESTAMPTZ '1601-01-01 00:00:00+00'
                + ($8 / 10000000) * INTERVAL '1 second'
                + (($8 / 10) % 1000000) * INTERVAL '1 microsecond'
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'contact'
          AND canonical_id = $3
          AND mapi_object_id = $4
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(canonical_id)
    .bind(object_id as i64)
    .bind(change_number as i64)
    .bind(&change_key)
    .bind(&predecessor_change_list)
    .bind(last_modification_time as i64)
    .execute(&mut **tx)
    .await?;

    Ok(Some(ExistingContactIdentityCommit {
        canonical_id,
        identity: AllocatedContactIdentity {
            object_id,
            change_number,
            change_key,
            predecessor_change_list,
        },
        last_modification_time,
        apply_imported_content: imported_wins,
        disposition: if conflict {
            MapiContactImportDisposition::ConflictResolved { imported_wins }
        } else {
            MapiContactImportDisposition::Applied
        },
    }))
}

fn imported_contact_version_wins_last_writer(
    incoming_last_modification_time: u64,
    incoming_change_key: &[u8],
    current_last_modification_time: u64,
    current_change_key: &[u8],
) -> Result<bool> {
    if !(17..=24).contains(&incoming_change_key.len())
        || !(17..=24).contains(&current_change_key.len())
    {
        bail!("invalid MAPI Contact ChangeKey XID length");
    }
    // [MS-OXCFXICS] section 3.1.5.6.2.2: modification time wins first;
    // equal versions are ordered by the ChangeKey NamespaceGuid.
    Ok(
        match incoming_last_modification_time.cmp(&current_last_modification_time) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => incoming_change_key[..16] >= current_change_key[..16],
        },
    )
}

type ContactPredecessors = BTreeMap<[u8; 16], Vec<u8>>;

fn parse_contact_predecessor_change_list(bytes: &[u8]) -> Result<ContactPredecessors> {
    let mut entries = ContactPredecessors::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let size = usize::from(bytes[offset]);
        offset += 1;
        let end = offset
            .checked_add(size)
            .ok_or_else(|| anyhow!("MAPI Contact PCL SizedXid length overflow"))?;
        let xid = bytes
            .get(offset..end)
            .ok_or_else(|| anyhow!("truncated MAPI Contact PCL XID"))?;
        offset = end;
        let (guid, local_id) = split_contact_xid(xid)?;
        match entries.get(&guid) {
            Some(existing) if existing.len() != local_id.len() => {
                bail!("MAPI Contact PCL LocalIds for one replica have inconsistent lengths")
            }
            Some(existing) if existing.as_slice() >= local_id => {}
            _ => {
                entries.insert(guid, local_id.to_vec());
            }
        }
    }
    if entries.is_empty() {
        bail!("MAPI Contact PCL cannot be empty");
    }
    Ok(entries)
}

fn split_contact_xid(bytes: &[u8]) -> Result<([u8; 16], &[u8])> {
    if !(17..=24).contains(&bytes.len()) {
        bail!("MAPI Contact XID length must be between 17 and 24 bytes");
    }
    let guid = bytes[..16]
        .try_into()
        .map_err(|_| anyhow!("MAPI Contact XID replica GUID is malformed"))?;
    Ok((guid, &bytes[16..]))
}

fn contact_predecessors_include(
    candidate: &ContactPredecessors,
    predecessor: &ContactPredecessors,
) -> Result<bool> {
    for (guid, predecessor_local_id) in predecessor {
        let Some(candidate_local_id) = candidate.get(guid) else {
            return Ok(false);
        };
        if candidate_local_id.len() != predecessor_local_id.len() {
            bail!("MAPI Contact PCL LocalIds for one replica have inconsistent lengths");
        }
        if candidate_local_id < predecessor_local_id {
            return Ok(false);
        }
    }
    Ok(true)
}

fn contact_predecessors_contain_change_key(
    entries: &ContactPredecessors,
    change_key: &[u8],
) -> Result<bool> {
    let (guid, local_id) = split_contact_xid(change_key)?;
    let Some(stored_local_id) = entries.get(&guid) else {
        return Ok(false);
    };
    if stored_local_id.len() != local_id.len() {
        bail!("MAPI Contact PCL and ChangeKey LocalIds have inconsistent lengths");
    }
    Ok(stored_local_id.as_slice() >= local_id)
}

fn merge_contact_predecessor_change_lists(
    mut current: ContactPredecessors,
    imported: ContactPredecessors,
) -> Result<Vec<u8>> {
    for (guid, imported_local_id) in imported {
        match current.get(&guid) {
            Some(existing) if existing.len() != imported_local_id.len() => {
                bail!("MAPI Contact PCL LocalIds for one replica have inconsistent lengths")
            }
            Some(existing) if existing >= &imported_local_id => {}
            _ => {
                current.insert(guid, imported_local_id);
            }
        }
    }
    let mut result = Vec::new();
    for (guid, local_id) in current {
        let xid_size = u8::try_from(guid.len() + local_id.len())
            .map_err(|_| anyhow!("MAPI Contact PCL XID is too large to serialize"))?;
        result.push(xid_size);
        result.extend_from_slice(&guid);
        result.extend_from_slice(&local_id);
    }
    Ok(result)
}

async fn lock_contact_replica_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    principal_account_id: Uuid,
) -> Result<Uuid> {
    sqlx::query(
        r#"
        INSERT INTO mapi_mailbox_replicas (
            tenant_id, account_id, replica_guid, next_global_counter
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tenant_id, account_id)
        DO UPDATE SET next_global_counter = GREATEST(
            mapi_mailbox_replicas.next_global_counter,
            $4
        )
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(Uuid::from_bytes(MAPI_STORE_REPLICA_GUID))
    .bind(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER as i64)
    .execute(&mut **tx)
    .await?;
    Ok(sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT replica_guid
        FROM mapi_mailbox_replicas
        WHERE tenant_id = $1 AND account_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .fetch_one(&mut **tx)
    .await?)
}

async fn allocate_next_contact_change_number_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    principal_account_id: Uuid,
    allocation_floor: u64,
) -> Result<u64> {
    sqlx::query(
        r#"
        UPDATE mapi_mailbox_replicas replica
        SET next_global_counter = GREATEST(
                replica.next_global_counter,
                COALESCE(
                    (
                        SELECT MAX(GREATEST(
                                   identity.mapi_global_counter,
                                   identity.mapi_change_number
                               )) + 1
                        FROM mapi_object_identities identity
                        WHERE identity.tenant_id = replica.tenant_id
                          AND identity.account_id = replica.account_id
                          AND identity.mapi_global_counter < $3
                          AND identity.mapi_change_number < $3
                    ),
                    $4
                ),
                $5
            ),
            updated_at = NOW()
        WHERE replica.tenant_id = $1 AND replica.account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
    .bind(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER as i64)
    .bind(allocation_floor as i64)
    .execute(&mut **tx)
    .await?;
    let change_number = sqlx::query_scalar::<_, i64>(
        r#"
        UPDATE mapi_mailbox_replicas
        SET next_global_counter = next_global_counter + 1,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
          AND next_global_counter >= $3
          AND next_global_counter < $4
        RETURNING next_global_counter - 1
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER as i64)
    .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow!("MAPI dynamic global counter space exhausted"))?;
    if change_number <= 0 || change_number as u64 > MAX_MAPI_GLOBAL_COUNTER {
        bail!("MAPI dynamic global counter space exhausted");
    }
    Ok(change_number as u64)
}

async fn allocate_contact_identity_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    principal_account_id: Uuid,
    mapi_folder_id: u64,
    contact_id: Uuid,
    imported_identity: Option<&MapiContactImportedIdentity>,
    last_modification_time: u64,
) -> Result<AllocatedContactIdentity> {
    let replica_guid = lock_contact_replica_in_tx(tx, tenant_id, principal_account_id).await?;

    let imported_source_counter = imported_identity
        .map(|identity| imported_source_counter(identity, replica_guid))
        .transpose()?;
    if let Some(source_counter) = imported_source_counter {
        let reserved = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT first_global_counter
            FROM mapi_local_replica_id_ranges
            WHERE tenant_id = $1
              AND account_id = $2
              AND replica_guid = $3
              AND first_global_counter <= $4
              AND end_global_counter_exclusive > $4
            LIMIT 1
            FOR SHARE
            "#,
        )
        .bind(tenant_id)
        .bind(principal_account_id)
        .bind(replica_guid)
        .bind(source_counter as i64)
        .fetch_optional(&mut **tx)
        .await?;
        if reserved.is_none() {
            bail!("MAPI Contact imported SourceKey was not locally reserved");
        }
        let deleted = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT min_global_counter
            FROM mapi_local_replica_deleted_ranges
            WHERE tenant_id = $1
              AND account_id = $2
              AND folder_id = $3
              AND replica_guid = $4
              AND min_global_counter <= $5
              AND max_global_counter >= $5
            LIMIT 1
            FOR SHARE
            "#,
        )
        .bind(tenant_id)
        .bind(principal_account_id)
        .bind(mapi_folder_id as i64)
        .bind(replica_guid)
        .bind(source_counter as i64)
        .fetch_optional(&mut **tx)
        .await?;
        if deleted.is_some() {
            return Err(MapiContactImportObjectDeleted.into());
        }
    }

    let allocation_floor = imported_source_counter
        .map(|counter| counter + 1)
        .unwrap_or(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER);
    let change_number = allocate_next_contact_change_number_in_tx(
        tx,
        tenant_id,
        principal_account_id,
        allocation_floor,
    )
    .await?;

    // [MS-OXCFXICS] sections 3.1.5.3, 3.2.5.9.4.2, and 3.3.5.2.1:
    // preserve the imported identity tuple while assigning a distinct server CN.
    let (source_counter, source_key, change_key, predecessor_change_list) =
        match (imported_source_counter, imported_identity) {
            (Some(source_counter), Some(identity)) => {
                if source_counter == change_number {
                    bail!("MAPI Contact imported SourceKey and server ChangeNumber must differ");
                }
                (
                    source_counter,
                    identity.source_key.clone(),
                    identity.change_key.clone(),
                    identity.predecessor_change_list.clone(),
                )
            }
            (None, None) => {
                let source_key = mapi_change_key(replica_guid, change_number);
                let predecessor_change_list = merge_predecessor_change_list(&[], &source_key)?;
                (
                    change_number,
                    source_key.clone(),
                    source_key,
                    predecessor_change_list,
                )
            }
            _ => unreachable!("validated imported Contact identity must be paired"),
        };
    let object_id = mapi_store_id(source_counter);
    let alias_collision = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM mapi_special_folder_aliases
            WHERE tenant_id = $1
              AND account_id = $2
              AND (alias_folder_id = $3 OR source_key = $4)
        )
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(object_id as i64)
    .bind(&source_key)
    .fetch_one(&mut **tx)
    .await?;
    if alias_collision {
        bail!("MAPI Contact identity collides with a special-folder alias");
    }
    let filetime = i64::try_from(last_modification_time)
        .map_err(|_| anyhow!("MAPI Contact FILETIME exceeds PostgreSQL range"))?;
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id,
            mapi_global_counter, mapi_object_id, source_key, change_key,
            instance_key, mapi_change_number, predecessor_change_list, updated_at
        )
        VALUES (
            $1, $2, 'contact', $3,
            $4, $5, $6, $7,
            $6, $8, $9,
            TIMESTAMPTZ '1601-01-01 00:00:00+00'
                + ($10 / 10000000) * INTERVAL '1 second'
                + (($10 / 10) % 1000000) * INTERVAL '1 microsecond'
        )
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(contact_id)
    .bind(source_counter as i64)
    .bind(object_id as i64)
    .bind(&source_key)
    .bind(&change_key)
    .bind(change_number as i64)
    .bind(&predecessor_change_list)
    .bind(filetime)
    .execute(&mut **tx)
    .await?;

    Ok(AllocatedContactIdentity {
        object_id,
        change_number,
        change_key,
        predecessor_change_list,
    })
}

fn imported_source_counter(
    identity: &MapiContactImportedIdentity,
    replica_guid: Uuid,
) -> Result<u64> {
    if identity.source_key.get(..16) != Some(replica_guid.as_bytes().as_slice()) {
        bail!("MAPI Contact imported SourceKey must use the local mailbox replica GUID");
    }
    // [MS-OXCFXICS] section 3.3.5.2.3: the client's foreign change
    // namespace cannot reuse a REPLGUID supplied by this server.
    if identity.change_key.get(..16) == Some(replica_guid.as_bytes().as_slice()) {
        bail!("MAPI Contact imported ChangeKey must not use the server replica GUID");
    }
    let mut counter_bytes = [0u8; 8];
    counter_bytes[2..].copy_from_slice(&identity.source_key[16..]);
    let counter = u64::from_be_bytes(counter_bytes);
    if !(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER..FIRST_RESERVED_HIGH_GLOBAL_COUNTER).contains(&counter) {
        bail!("MAPI Contact imported SourceKey GLOBCNT is outside the dynamic local range");
    }
    Ok(counter)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn imported_identity(change_key: Vec<u8>) -> MapiContactImportedIdentity {
        let mut predecessor_change_list = vec![change_key.len() as u8];
        predecessor_change_list.extend_from_slice(&change_key);
        MapiContactImportedIdentity {
            source_key: mapi_change_key(Uuid::from_bytes(MAPI_STORE_REPLICA_GUID), 500),
            change_key,
            predecessor_change_list,
            last_modification_time: 134_128_518_000_000_000,
        }
    }

    #[test]
    fn imported_contact_change_key_must_use_a_foreign_identifier() {
        let gid_sized = mapi_change_key(Uuid::from_u128(2), 9);
        assert_eq!(gid_sized.len(), 22);
        assert_eq!(
            validate_imported_identity(&imported_identity(gid_sized))
                .unwrap_err()
                .to_string(),
            "MAPI Contact imported ChangeKey must be a foreign XID"
        );

        let mut server_namespace = MAPI_STORE_REPLICA_GUID.to_vec();
        server_namespace.extend_from_slice(&9u32.to_be_bytes());
        let identity = imported_identity(server_namespace);
        validate_imported_identity(&identity).unwrap();
        assert_eq!(
            imported_source_counter(&identity, Uuid::from_bytes(MAPI_STORE_REPLICA_GUID))
                .unwrap_err()
                .to_string(),
            "MAPI Contact imported ChangeKey must not use the server replica GUID"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_contact_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    contact_book_id: Uuid,
    contact_id: Uuid,
    contact: &NormalizedContact,
    last_modification_time: u64,
) -> Result<()> {
    let filetime = i64::try_from(last_modification_time)
        .map_err(|_| anyhow!("MAPI Contact FILETIME exceeds PostgreSQL range"))?;
    sqlx::query(
        r#"
        INSERT INTO contacts (
            id, tenant_id, owner_account_id, contact_book_id, uid,
            display_name, name_prefix, given_name, middle_name, family_name, name_suffix,
            nickname, phonetic_given_name, phonetic_family_name, job_title, role,
            organization_name, organization_unit, emails_json, phones_json, addresses_json,
            urls_json, notes, raw_vcard, import_source, source_uid, source_etag,
            source_payload_json, updated_at
        )
        VALUES (
            $1, $2, $3, $4, $1::text,
            $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15,
            $16, $17, $18, $19, $20,
            $21, $22, $23, 'mapi', $24, $25,
            $26,
            TIMESTAMPTZ '1601-01-01 00:00:00+00'
                + ($27 / 10000000) * INTERVAL '1 second'
                + (($27 / 10) % 1000000) * INTERVAL '1 microsecond'
        )
        "#,
    )
    .bind(contact_id)
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(contact_book_id)
    .bind(&contact.name)
    .bind(&contact.structured_name.prefix)
    .bind(&contact.structured_name.given)
    .bind(&contact.structured_name.middle)
    .bind(&contact.structured_name.family)
    .bind(&contact.structured_name.suffix)
    .bind(&contact.structured_name.nickname)
    .bind(&contact.structured_name.phonetic_given)
    .bind(&contact.structured_name.phonetic_family)
    .bind(&contact.job_title)
    .bind(&contact.role)
    .bind(&contact.organization_name)
    .bind(&contact.team)
    .bind(&contact.emails_json)
    .bind(&contact.phones_json)
    .bind(&contact.addresses_json)
    .bind(&contact.urls_json)
    .bind(&contact.notes)
    .bind(contact.raw_vcard.as_deref())
    .bind(contact.source_uid.as_deref())
    .bind(contact.source_etag.as_deref())
    .bind(&contact.source_payload_json)
    .bind(filetime)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn update_contact_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    contact_book_id: Uuid,
    contact_id: Uuid,
    contact: &NormalizedContact,
    last_modification_time: u64,
) -> Result<()> {
    let filetime = i64::try_from(last_modification_time)
        .map_err(|_| anyhow!("MAPI Contact FILETIME exceeds PostgreSQL range"))?;
    let updated = sqlx::query(
        r#"
        UPDATE contacts
        SET display_name = $5,
            name_prefix = $6,
            given_name = $7,
            middle_name = $8,
            family_name = $9,
            name_suffix = $10,
            nickname = $11,
            phonetic_given_name = $12,
            phonetic_family_name = $13,
            job_title = $14,
            role = $15,
            organization_name = $16,
            organization_unit = $17,
            emails_json = $18,
            phones_json = $19,
            addresses_json = $20,
            urls_json = $21,
            notes = $22,
            raw_vcard = $23,
            import_source = 'mapi',
            source_uid = $24,
            source_etag = $25,
            source_payload_json = $26,
            updated_at = TIMESTAMPTZ '1601-01-01 00:00:00+00'
                + ($27 / 10000000) * INTERVAL '1 second'
                + (($27 / 10) % 1000000) * INTERVAL '1 microsecond'
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND contact_book_id = $3
          AND id = $4
        "#,
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(contact_book_id)
    .bind(contact_id)
    .bind(&contact.name)
    .bind(&contact.structured_name.prefix)
    .bind(&contact.structured_name.given)
    .bind(&contact.structured_name.middle)
    .bind(&contact.structured_name.family)
    .bind(&contact.structured_name.suffix)
    .bind(&contact.structured_name.nickname)
    .bind(&contact.structured_name.phonetic_given)
    .bind(&contact.structured_name.phonetic_family)
    .bind(&contact.job_title)
    .bind(&contact.role)
    .bind(&contact.organization_name)
    .bind(&contact.team)
    .bind(&contact.emails_json)
    .bind(&contact.phones_json)
    .bind(&contact.addresses_json)
    .bind(&contact.urls_json)
    .bind(&contact.notes)
    .bind(contact.raw_vcard.as_deref())
    .bind(contact.source_uid.as_deref())
    .bind(contact.source_etag.as_deref())
    .bind(&contact.source_payload_json)
    .bind(filetime)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        bail!("canonical MAPI Contact disappeared before imported update");
    }
    Ok(())
}

async fn insert_custom_properties_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    principal_account_id: Uuid,
    contact_id: Uuid,
    values: &[MapiContactCustomPropertyValue],
) -> Result<()> {
    for value in values {
        sqlx::query(
            r#"
            INSERT INTO mapi_custom_property_values (
                tenant_id, account_id, object_kind, canonical_id,
                property_tag, property_type, property_value
            )
            VALUES ($1, $2, 'contact', $3, $4, $5, $6)
            "#,
        )
        .bind(tenant_id)
        .bind(principal_account_id)
        .bind(contact_id)
        .bind(i64::from(value.property_tag))
        .bind(i32::from(value.property_type))
        .bind(&value.property_value)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

async fn upsert_custom_properties_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    principal_account_id: Uuid,
    contact_id: Uuid,
    values: &[MapiContactCustomPropertyValue],
) -> Result<()> {
    for value in values {
        sqlx::query(
            r#"
            INSERT INTO mapi_custom_property_values (
                tenant_id, account_id, object_kind, canonical_id,
                property_tag, property_type, property_value
            )
            VALUES ($1, $2, 'contact', $3, $4, $5, $6)
            ON CONFLICT (
                tenant_id, account_id, object_kind, canonical_id,
                property_tag, property_type
            )
            DO UPDATE SET property_value = EXCLUDED.property_value,
                          updated_at = NOW()
            "#,
        )
        .bind(tenant_id)
        .bind(principal_account_id)
        .bind(contact_id)
        .bind(i64::from(value.property_tag))
        .bind(i32::from(value.property_type))
        .bind(&value.property_value)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn record_contact_change_in_tx(
    storage: &Storage,
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    contact_book_id: Uuid,
    contact_id: Uuid,
    change_kind: &str,
    created: bool,
    custom_properties_changed: bool,
    mapi_change_number: u64,
) -> Result<()> {
    let modseq = storage
        .allocate_account_modseq_in_tx(
            tx,
            tenant_id,
            owner_account_id,
            CanonicalChangeCategory::Contacts.as_str(),
        )
        .await?;
    set_created_contact_modseq_in_tx(
        tx,
        tenant_id,
        owner_account_id,
        contact_book_id,
        contact_id,
        modseq,
    )
    .await?;
    let affected_principals =
        contact_affected_principals_in_tx(tx, tenant_id, owner_account_id, contact_book_id).await?;
    Storage::insert_mail_change_log_in_tx(
        tx,
        tenant_id,
        Some(owner_account_id),
        None,
        "contact",
        contact_id,
        change_kind,
        modseq,
        &affected_principals,
        serde_json::json!({
            "collectionId": contact_book_id,
            "objectUid": contact_id.to_string(),
            "created": created,
            "customPropertiesChanged": custom_properties_changed,
            "mapiChangeNumber": mapi_change_number,
        }),
    )
    .await?;
    Storage::emit_collaboration_change(
        tx,
        tenant_id,
        CanonicalChangeCategory::Contacts,
        owner_account_id,
    )
    .await?;
    Ok(())
}

async fn set_created_contact_modseq_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    contact_book_id: Uuid,
    contact_id: Uuid,
    modseq: i64,
) -> Result<()> {
    let updated = sqlx::query(
        r#"
        UPDATE contacts
        SET modseq = $5
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND contact_book_id = $3
          AND id = $4
        "#,
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(contact_book_id)
    .bind(contact_id)
    .bind(modseq)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        bail!("canonical MAPI Contact disappeared before version assignment");
    }
    sqlx::query(
        r#"
        UPDATE contact_books
        SET sync_modseq = GREATEST(sync_modseq, $4),
            updated_at = NOW()
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND id = $3
        "#,
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(contact_book_id)
    .bind(modseq)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn contact_affected_principals_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    contact_book_id: Uuid,
) -> Result<Vec<Uuid>> {
    let mut principals = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT grantee_account_id
        FROM contact_book_grants
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND contact_book_id = $3
          AND may_read
        "#,
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(contact_book_id)
    .fetch_all(&mut **tx)
    .await?;
    principals.push(owner_account_id);
    principals.sort();
    principals.dedup();
    Ok(principals)
}
