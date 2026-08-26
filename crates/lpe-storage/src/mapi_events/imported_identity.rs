use anyhow::{anyhow, bail, Result};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use super::{
    mapi_change_key, mapi_store_id, merge_predecessor_change_list, EventIdentityVersion,
    MapiEventImportedIdentity, FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER,
    FIRST_RESERVED_HIGH_GLOBAL_COUNTER,
};
use crate::mapi_store_identity::{
    allocate_mapi_store_global_counter_in_tx, ensure_mapi_mailbox_replica_in_tx,
    ensure_mapi_store_identity_in_tx,
};

pub(super) fn normalize_mapi_event_filetime(value: u64) -> Result<u64> {
    if value > i64::MAX as u64 {
        bail!("MAPI Event LastModificationTime is outside the PostgreSQL FILETIME range");
    }
    Ok(value - value % 10)
}

pub(super) async fn current_mapi_event_filetime_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
) -> Result<u64> {
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
        bail!("PostgreSQL returned an invalid current MAPI Event FILETIME");
    }
    Ok(value as u64)
}

pub(super) fn validate_imported_identity(identity: &MapiEventImportedIdentity) -> Result<()> {
    if identity.source_key.len() != 22 {
        bail!("MAPI Event imported SourceKey must be exactly 22 bytes");
    }
    if !(17..=24).contains(&identity.change_key.len()) {
        bail!("MAPI Event imported ChangeKey XID must be between 17 and 24 bytes");
    }
    let normalized =
        merge_predecessor_change_list(&identity.predecessor_change_list, &identity.change_key)
            .map_err(|_| {
                anyhow!("MAPI Event imported PCL must be canonical and contain its ChangeKey")
            })?;
    if normalized != identity.predecessor_change_list {
        bail!("MAPI Event imported PCL must be canonical and contain its ChangeKey");
    }
    normalize_mapi_event_filetime(identity.last_modification_time)?;
    Ok(())
}

pub(super) fn imported_source_global_counter(
    identity: &MapiEventImportedIdentity,
    replica_guid: Uuid,
) -> Result<u64> {
    validate_imported_identity(identity)?;
    if identity.source_key.get(..16) != Some(replica_guid.as_bytes().as_slice()) {
        bail!("MAPI Event imported SourceKey must use the local mailbox replica GUID");
    }
    let mut counter_bytes = [0u8; 8];
    counter_bytes[2..].copy_from_slice(&identity.source_key[16..]);
    let global_counter = u64::from_be_bytes(counter_bytes);
    if !(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER..FIRST_RESERVED_HIGH_GLOBAL_COUNTER)
        .contains(&global_counter)
    {
        bail!("MAPI Event imported SourceKey GLOBCNT is outside the dynamic local range");
    }
    Ok(global_counter)
}

pub(super) async fn allocate_mapi_event_identity_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    principal_account_id: Uuid,
    event_id: Uuid,
    imported_identity: Option<&MapiEventImportedIdentity>,
) -> Result<(u64, EventIdentityVersion)> {
    let store_identity = ensure_mapi_store_identity_in_tx(tx).await?;
    ensure_mapi_mailbox_replica_in_tx(tx, *tenant_id, principal_account_id, store_identity).await?;
    let replica_guid = store_identity.replica_guid;
    let imported_source_counter = imported_identity
        .map(|identity| imported_source_global_counter(identity, replica_guid))
        .transpose()?;
    if let Some(source_counter) = imported_source_counter {
        let reserved = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM mapi_local_replica_id_ranges
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND replica_guid = $3
                  AND first_global_counter <= $4
                  AND end_global_counter_exclusive > $4
            )
            "#,
        )
        .bind(tenant_id)
        .bind(principal_account_id)
        .bind(replica_guid)
        .bind(source_counter as i64)
        .fetch_one(&mut **tx)
        .await?;
        if !reserved {
            bail!("MAPI Event imported SourceKey was not locally reserved");
        }
        let retired = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM mapi_calendar_event_identity_retirements
                WHERE old_source_key = $1
            )
            "#,
        )
        .bind(
            &imported_identity
                .expect("paired imported identity")
                .source_key,
        )
        .fetch_one(&mut **tx)
        .await?;
        if retired {
            bail!("MAPI Event imported SourceKey was already retired");
        }
    }
    let (_, change_number) = allocate_mapi_store_global_counter_in_tx(tx).await?;

    // [MS-OXCFXICS] 3.1.5.3 assigns the imported version its supplied CK/PCL
    // while allocating a distinct server-internal CN for MetaTagCnsetSeen.
    // Only a later direct modification replaces that foreign CK with the XID
    // for its new server CN.
    let (source_global_counter, source_key, change_key, predecessor_change_list) =
        match (imported_source_counter, imported_identity) {
            (Some(source_global_counter), Some(identity)) => {
                if source_global_counter == change_number {
                    bail!("MAPI Event imported SourceKey and server ChangeNumber must differ");
                }
                (
                    source_global_counter,
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
            _ => unreachable!("validated imported identity state must be paired"),
        };
    let object_id = mapi_store_id(source_global_counter);
    let last_modification_time = match imported_identity {
        Some(identity) => normalize_mapi_event_filetime(identity.last_modification_time)?,
        None => current_mapi_event_filetime_in_tx(tx).await?,
    };
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id,
            mapi_global_counter, mapi_object_id, source_key, change_key,
            instance_key, mapi_change_number, predecessor_change_list, updated_at
        )
        VALUES (
            $1, $2, 'calendar_event', $3, $4, $5, $6, $7, $6, $8, $9,
            TIMESTAMPTZ '1601-01-01 00:00:00+00'
                + ($10 / 10000000) * INTERVAL '1 second'
                + (($10 / 10) % 1000000) * INTERVAL '1 microsecond'
        )
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(event_id)
    .bind(source_global_counter as i64)
    .bind(object_id as i64)
    .bind(&source_key)
    .bind(&change_key)
    .bind(change_number as i64)
    .bind(&predecessor_change_list)
    .bind(last_modification_time as i64)
    .execute(&mut **tx)
    .await?;
    Ok((
        object_id,
        EventIdentityVersion {
            account_id: principal_account_id,
            mapi_object_id: object_id,
            retired_mapi_object_id: None,
            change_number,
            change_key,
            predecessor_change_list,
            last_modification_time,
        },
    ))
}

pub(super) async fn rotate_active_mapi_event_identities_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    event_id: Uuid,
) -> Result<Vec<EventIdentityVersion>> {
    rotate_mapi_event_identities_in_tx(tx, tenant_id, event_id, "calendar_event", None, None).await
}

pub(super) async fn rotate_mapi_event_identities_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    event_id: Uuid,
    object_kind: &str,
    imported_principal_account_id: Option<Uuid>,
    imported_identity: Option<&MapiEventImportedIdentity>,
) -> Result<Vec<EventIdentityVersion>> {
    if let Some(identity) = imported_identity {
        validate_imported_identity(identity)?;
    }
    let identities = sqlx::query(
        r#"
        SELECT
            account_id,
            mapi_global_counter,
            mapi_object_id,
            source_key,
            mapi_change_number,
            predecessor_change_list
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND object_kind = $2
          AND canonical_id = $3
          AND deleted_at IS NULL
        ORDER BY account_id
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(object_kind)
    .bind(event_id)
    .fetch_all(&mut **tx)
    .await?;
    let mut versions = Vec::with_capacity(identities.len());
    let mut imported_identity_applied = false;
    for identity in identities {
        let account_id = identity.get::<Uuid, _>("account_id");
        let source_key = identity.get::<Vec<u8>, _>("source_key");
        let principal_imported_identity =
            imported_identity.filter(|_| imported_principal_account_id == Some(account_id));
        if object_kind != "calendar_event"
            && principal_imported_identity.is_some_and(|imported| imported.source_key != source_key)
        {
            bail!("MAPI Event SourceKey changed before the imported update");
        }
        let current_mapi_global_counter = identity.get::<i64, _>("mapi_global_counter");
        if current_mapi_global_counter <= 0
            || current_mapi_global_counter as u64 >= FIRST_RESERVED_HIGH_GLOBAL_COUNTER
        {
            bail!("stored MAPI Event global counter is outside the dynamic range");
        }
        let current_mapi_global_counter = current_mapi_global_counter as u64;
        let current_mapi_object_id = identity.get::<i64, _>("mapi_object_id");
        if current_mapi_object_id <= 0 {
            bail!("stored MAPI Event object id is outside the dynamic range");
        }
        let current_mapi_object_id = current_mapi_object_id as u64;
        let current_change_number = identity.get::<i64, _>("mapi_change_number");
        if current_change_number <= 0
            || current_change_number as u64 >= FIRST_RESERVED_HIGH_GLOBAL_COUNTER
        {
            bail!("stored MAPI Event change number is outside the dynamic GLOBCNT range");
        }
        let predecessor_change_list = identity.get::<Vec<u8>, _>("predecessor_change_list");
        let (store_identity, change_number) = allocate_mapi_store_global_counter_in_tx(tx).await?;
        ensure_mapi_mailbox_replica_in_tx(tx, *tenant_id, account_id, store_identity).await?;
        let imported_source_counter = principal_imported_identity
            .map(|identity| imported_source_global_counter(identity, store_identity.replica_guid))
            .transpose()?;
        if let Some(source_counter) = imported_source_counter {
            let imported = principal_imported_identity.expect("validated imported identity");
            let reserved = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM mapi_local_replica_id_ranges
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND replica_guid = $3
                      AND first_global_counter <= $4
                      AND end_global_counter_exclusive > $4
                )
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(store_identity.replica_guid)
            .bind(source_counter as i64)
            .fetch_one(&mut **tx)
            .await?;
            if !reserved {
                bail!("MAPI Event imported replacement SourceKey was not locally reserved");
            }
            let retired = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM mapi_calendar_event_identity_retirements
                    WHERE old_source_key = $1
                )
                "#,
            )
            .bind(&imported.source_key)
            .fetch_one(&mut **tx)
            .await?;
            if retired {
                bail!("MAPI Event imported replacement SourceKey was already retired");
            }
            if source_counter == change_number {
                bail!(
                    "MAPI Event imported replacement SourceKey and server ChangeNumber must differ"
                );
            }
        }
        let (
            mapi_global_counter,
            mapi_object_id,
            committed_source_key,
            retired_mapi_object_id,
            change_key,
            predecessor_change_list,
            last_modification_time,
        ) = if let Some(imported) = principal_imported_identity {
            imported_identity_applied = true;
            (
                imported_source_counter.expect("validated imported SourceKey counter"),
                mapi_store_id(
                    imported_source_counter.expect("validated imported SourceKey counter"),
                ),
                imported.source_key.clone(),
                (source_key != imported.source_key).then_some(current_mapi_object_id),
                imported.change_key.clone(),
                imported.predecessor_change_list.clone(),
                normalize_mapi_event_filetime(imported.last_modification_time)?,
            )
        } else {
            let change_key = mapi_change_key(store_identity.replica_guid, change_number);
            let predecessor_change_list =
                merge_predecessor_change_list(&predecessor_change_list, &change_key)?;
            (
                current_mapi_global_counter,
                current_mapi_object_id,
                source_key.clone(),
                None,
                change_key,
                predecessor_change_list,
                current_mapi_event_filetime_in_tx(tx).await?,
            )
        };
        if let Some(retired_object_id) = retired_mapi_object_id {
            sqlx::query(
                r#"
                INSERT INTO mapi_calendar_event_identity_retirements (
                    tenant_id,
                    account_id,
                    event_id,
                    old_mapi_object_id,
                    replacement_mapi_object_id,
                    old_source_key,
                    replacement_source_key,
                    retired_change_number
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(event_id)
            .bind(retired_object_id as i64)
            .bind(mapi_object_id as i64)
            .bind(&source_key)
            .bind(&committed_source_key)
            .bind(change_number as i64)
            .execute(&mut **tx)
            .await?;
        }
        let updated = sqlx::query(
            r#"
            UPDATE mapi_object_identities
            SET mapi_global_counter = $5,
                mapi_object_id = $6,
                source_key = $7,
                instance_key = $7,
                mapi_change_number = $8,
                change_key = $9,
                predecessor_change_list = $10,
                updated_at = TIMESTAMPTZ '1601-01-01 00:00:00+00'
                    + ($11 / 10000000) * INTERVAL '1 second'
                    + (($11 / 10) % 1000000) * INTERVAL '1 microsecond'
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = $3
              AND canonical_id = $4
              AND deleted_at IS NULL
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(object_kind)
        .bind(event_id)
        .bind(mapi_global_counter as i64)
        .bind(mapi_object_id as i64)
        .bind(&committed_source_key)
        .bind(change_number as i64)
        .bind(&change_key)
        .bind(&predecessor_change_list)
        .bind(last_modification_time as i64)
        .execute(&mut **tx)
        .await?;
        if updated.rows_affected() != 1 {
            bail!("MAPI Event identity disappeared during version rotation");
        }
        versions.push(EventIdentityVersion {
            account_id,
            mapi_object_id,
            retired_mapi_object_id,
            change_number,
            change_key,
            predecessor_change_list,
            last_modification_time,
        });
    }
    if imported_identity.is_some() && !imported_identity_applied {
        bail!("principal MAPI Event identity was not found for the imported update");
    }
    Ok(versions)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn realistic_imported_identity() -> MapiEventImportedIdentity {
        let change_key = vec![
            0x67, 0x45, 0x48, 0x20, 0x69, 0x60, 0xca, 0x40, 0x9d, 0x80, 0x08, 0x17, 0x06, 0x0f,
            0xa2, 0xc1, 0x00, 0x00, 0x04, 0x57,
        ];
        let mut predecessor_change_list = vec![change_key.len() as u8];
        predecessor_change_list.extend_from_slice(&change_key);
        MapiEventImportedIdentity {
            source_key: mapi_change_key(Uuid::from_u128(2), 0x0df8_974b_7f66),
            change_key,
            predecessor_change_list,
            last_modification_time: 134_128_518_000_000_000,
        }
    }

    #[test]
    fn imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key() {
        let mut identity = realistic_imported_identity();
        assert!(validate_imported_identity(&identity).is_ok());
        assert_eq!(
            imported_source_global_counter(&identity, Uuid::from_u128(2)).unwrap(),
            0x0df8_974b_7f66
        );

        let other_change_key = mapi_change_key(Uuid::from_u128(1), 9);
        identity.predecessor_change_list =
            merge_predecessor_change_list(&identity.predecessor_change_list, &other_change_key)
                .unwrap();
        assert!(validate_imported_identity(&identity).is_ok());

        identity.predecessor_change_list = vec![other_change_key.len() as u8];
        identity
            .predecessor_change_list
            .extend_from_slice(&other_change_key);
        assert_eq!(
            validate_imported_identity(&identity)
                .unwrap_err()
                .to_string(),
            "MAPI Event imported PCL must be canonical and contain its ChangeKey"
        );
    }

    #[test]
    fn imported_identity_rejects_a_foreign_source_key_replica() {
        let identity = realistic_imported_identity();

        assert_eq!(
            imported_source_global_counter(&identity, Uuid::from_u128(1))
                .unwrap_err()
                .to_string(),
            "MAPI Event imported SourceKey must use the local mailbox replica GUID"
        );
    }
}
