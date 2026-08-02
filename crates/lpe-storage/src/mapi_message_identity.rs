use anyhow::{bail, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    mapi_events::{mapi_change_key, merge_predecessor_change_list},
    mapi_store_identity::{
        allocate_mapi_store_global_counter_in_tx, ensure_mapi_mailbox_replica_in_tx, mapi_store_id,
        mapi_xid, MapiMessageIdentityMove, MAPI_FIRST_GLOBAL_COUNTER,
        MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER, MAPI_MAX_GLOBAL_COUNTER,
    },
};

/// [MS-OXCFXICS] sections 2.2.1.2.3, 2.2.1.2.7, 2.2.1.2.8, and 3.1.5.3
/// require a new durable server version after a direct normal-message change.
pub(crate) async fn rotate_active_mapi_message_identity_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: &Uuid,
    account_id: Uuid,
    message_id: Uuid,
) -> Result<()> {
    let identity = sqlx::query(
        r#"
        SELECT mapi_change_number, predecessor_change_list
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'message'
          AND canonical_id = $3
          AND deleted_at IS NULL
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(message_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(identity) = identity else {
        return Ok(());
    };
    let current_change_number = identity.try_get::<i64, _>("mapi_change_number")?;
    if current_change_number < MAPI_FIRST_GLOBAL_COUNTER as i64
        || current_change_number >= MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64
    {
        bail!("stored MAPI message change number is outside the dynamic GLOBCNT range");
    }
    let predecessor_change_list = identity.try_get::<Vec<u8>, _>("predecessor_change_list")?;
    let (store_identity, change_number) = allocate_mapi_store_global_counter_in_tx(tx).await?;
    if change_number > MAPI_MAX_GLOBAL_COUNTER
        || change_number >= MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER
    {
        bail!("MAPI dynamic global counter space exhausted");
    }
    ensure_mapi_mailbox_replica_in_tx(tx, *tenant_id, account_id, store_identity).await?;
    let change_key = mapi_change_key(store_identity.replica_guid, change_number);
    let predecessor_change_list =
        merge_predecessor_change_list(&predecessor_change_list, &change_key)?;
    let updated = sqlx::query(
        r#"
        UPDATE mapi_object_identities
        SET mapi_change_number = $4,
            change_key = $5,
            predecessor_change_list = $6,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'message'
          AND canonical_id = $3
          AND deleted_at IS NULL
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(message_id)
    .bind(change_number as i64)
    .bind(&change_key)
    .bind(&predecessor_change_list)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        bail!("active MAPI message identity disappeared during version rotation");
    }
    Ok(())
}

/// [MS-OXCFXICS] section 3.1.5.3 requires a move to create a new message
/// object identity. The source tombstone retains the returned old object ID.
pub(crate) async fn rekey_active_mapi_message_identity_for_server_move_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: &Uuid,
    account_id: Uuid,
    message_id: Uuid,
) -> Result<Option<MapiMessageIdentityMove>> {
    let identity = sqlx::query(
        r#"
        SELECT
            mapi_object_id,
            source_key,
            mapi_change_number,
            change_key,
            predecessor_change_list
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'message'
          AND canonical_id = $3
          AND deleted_at IS NULL
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(message_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(identity) = identity else {
        return Ok(None);
    };
    let old_mapi_object_id = u64::try_from(identity.try_get::<i64, _>("mapi_object_id")?)
        .map_err(|_| anyhow::anyhow!("stored MAPI message object id is invalid"))?;
    let old_source_key = identity.try_get::<Vec<u8>, _>("source_key")?;
    let old_change_number = u64::try_from(identity.try_get::<i64, _>("mapi_change_number")?)
        .map_err(|_| anyhow::anyhow!("stored MAPI message change number is invalid"))?;
    let old_change_key = identity.try_get::<Vec<u8>, _>("change_key")?;
    let predecessor_change_list = identity.try_get::<Vec<u8>, _>("predecessor_change_list")?;
    let (store_identity, new_global_counter) = allocate_mapi_store_global_counter_in_tx(tx).await?;
    if new_global_counter > MAPI_MAX_GLOBAL_COUNTER
        || new_global_counter >= MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER
    {
        bail!("MAPI dynamic global counter space exhausted");
    }
    ensure_mapi_mailbox_replica_in_tx(tx, *tenant_id, account_id, store_identity).await?;
    let new_mapi_object_id = mapi_store_id(new_global_counter);
    let new_source_key = mapi_xid(store_identity.replica_guid, new_global_counter);
    let (_, new_change_number) = allocate_mapi_store_global_counter_in_tx(tx).await?;
    if new_change_number > MAPI_MAX_GLOBAL_COUNTER
        || new_change_number >= MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER
    {
        bail!("MAPI dynamic global counter space exhausted");
    }
    let new_change_key = mapi_change_key(store_identity.replica_guid, new_change_number);
    let new_predecessor_change_list =
        merge_predecessor_change_list(&predecessor_change_list, &new_change_key)?;
    let updated = sqlx::query(
        r#"
        UPDATE mapi_object_identities
        SET mapi_global_counter = $5,
            mapi_object_id = $6,
            source_key = $7,
            change_key = $8,
            instance_key = $7,
            mapi_change_number = $9,
            predecessor_change_list = $10,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'message'
          AND canonical_id = $3
          AND source_key = $4
          AND mapi_object_id = $11
          AND deleted_at IS NULL
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(message_id)
    .bind(&old_source_key)
    .bind(new_global_counter as i64)
    .bind(new_mapi_object_id as i64)
    .bind(&new_source_key)
    .bind(&new_change_key)
    .bind(new_change_number as i64)
    .bind(&new_predecessor_change_list)
    .bind(old_mapi_object_id as i64)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        bail!("active MAPI message identity disappeared during server move");
    }
    Ok(Some(MapiMessageIdentityMove {
        old_mapi_object_id,
        new_mapi_object_id,
        old_source_key,
        new_source_key,
        old_change_number,
        new_change_number,
        old_change_key,
        new_change_key,
    }))
}
