use anyhow::{anyhow, bail, Result};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{JmapEmail, Storage};

// [MS-OXCSTOR] section 2.2.1.8.2: a MAPI object ID combines a nonzero
// REPLID with a 48-bit GLOBCNT. LPE uses REPLID 1 for its local store.
// Section 3.2.3 requires a newly created database to receive a randomly
// generated REPLGUID, seeded by the canonical schema before startup.
pub const MAPI_FIRST_GLOBAL_COUNTER: u64 = 43;
pub const MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER: u64 = 0x7FFF_FE00_0000;
pub const MAPI_MAX_GLOBAL_COUNTER: u64 = 0x7FFF_FFFF_FFFF;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapiStoreIdentity {
    pub replica_guid: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapiMessageImportedMoveIdentity {
    pub expected_source_key: Vec<u8>,
    pub destination_source_key: Vec<u8>,
    pub change_key: Vec<u8>,
    pub predecessor_change_list: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapiMessageIdentityMove {
    pub old_mapi_object_id: u64,
    pub new_mapi_object_id: u64,
    pub old_source_key: Vec<u8>,
    pub new_source_key: Vec<u8>,
    pub old_change_number: u64,
    pub new_change_number: u64,
    pub old_change_key: Vec<u8>,
    pub new_change_key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct MapiMessageMoveResult {
    pub email: JmapEmail,
    pub identity: MapiMessageIdentityMove,
}

impl Storage {
    pub async fn fetch_mapi_store_identity(&self) -> Result<MapiStoreIdentity> {
        let mut tx = self.pool.begin().await?;
        let identity = ensure_mapi_store_identity_in_tx(&mut tx).await?;
        tx.commit().await?;
        Ok(identity)
    }
}

pub async fn ensure_mapi_store_identity_in_tx(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<MapiStoreIdentity> {
    let replica_guid = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT replica_guid
        FROM mapi_store_identity
        WHERE singleton = TRUE
        FOR UPDATE
        "#,
    )
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| {
        anyhow!(
            "MAPI store identity is missing; initialize a fresh LPE database from crates/lpe-storage/sql/schema.sql"
        )
    })?;
    Ok(MapiStoreIdentity { replica_guid })
}

pub async fn ensure_mapi_mailbox_replica_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    store_identity: MapiStoreIdentity,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO mapi_mailbox_replicas (tenant_id, account_id, replica_guid)
        VALUES ($1, $2, $3)
        ON CONFLICT (tenant_id, account_id) DO NOTHING
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(store_identity.replica_guid)
    .execute(&mut **tx)
    .await?;

    let replica_guid = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT replica_guid
        FROM mapi_mailbox_replicas
        WHERE tenant_id = $1
          AND account_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow!("MAPI mailbox replica was not initialized"))?;
    if replica_guid != store_identity.replica_guid {
        bail!("MAPI mailbox replica GUID differs from the database MAPI store identity");
    }
    Ok(())
}

pub async fn allocate_mapi_store_global_counter_in_tx(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(MapiStoreIdentity, u64)> {
    let store_identity = ensure_mapi_store_identity_in_tx(tx).await?;
    let counter = sqlx::query_scalar::<_, i64>(
        r#"
        UPDATE mapi_store_identity
        SET next_global_counter = next_global_counter + 1,
            updated_at = NOW()
        WHERE singleton = TRUE
          AND next_global_counter < $1
        RETURNING next_global_counter - 1
        "#,
    )
    .bind(MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow!("MAPI global counter space exhausted"))?;
    let counter = u64::try_from(counter).map_err(|_| anyhow!("invalid MAPI global counter"))?;
    Ok((store_identity, counter))
}

pub async fn reserve_mapi_store_global_counter_range_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    count: u32,
) -> Result<(MapiStoreIdentity, u64)> {
    if count == 0 {
        bail!("invalid MAPI local replica ID count: {count}");
    }
    let store_identity = ensure_mapi_store_identity_in_tx(tx).await?;
    let count = i64::from(count);
    let first_counter = sqlx::query_scalar::<_, i64>(
        r#"
        UPDATE mapi_store_identity
        SET next_global_counter = next_global_counter + $1,
            updated_at = NOW()
        WHERE singleton = TRUE
          AND next_global_counter <= $2 - $1
        RETURNING next_global_counter - $1
        "#,
    )
    .bind(count)
    .bind(MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow!("MAPI local replica ID space exhausted"))?;
    let first_counter =
        u64::try_from(first_counter).map_err(|_| anyhow!("invalid MAPI global counter"))?;
    Ok((store_identity, first_counter))
}

pub const fn mapi_store_id(global_counter: u64) -> u64 {
    ((global_counter & 0x0000_FFFF_FFFF_FFFF) << 16) | 1
}

pub fn mapi_xid(replica_guid: Uuid, global_counter: u64) -> Vec<u8> {
    let mut value = replica_guid.as_bytes().to_vec();
    value.extend_from_slice(&global_counter.to_be_bytes()[2..]);
    value
}
