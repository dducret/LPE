use anyhow::{anyhow, bail, Result};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    mapi_events::{mapi_change_key, mapi_store_id, merge_predecessor_change_list},
    CanonicalChangeCategory, Storage,
};

use super::{
    AccessibleEvent, MapiEventIdentityMove, MapiEventImportedMoveIdentity,
    MoveAccessibleEventToDeletedItemsResult,
};

const MAX_MAPI_GLOBAL_COUNTER: u64 = 0x7FFF_FFFF_FFFF;
const FIRST_RESERVED_HIGH_GLOBAL_COUNTER: u64 = 0x7FFF_FE00_0000;
const FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER: u64 = 43;

impl Storage {
    pub async fn fetch_accessible_deleted_events(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<AccessibleEvent>> {
        self.fetch_accessible_events_internal(principal_account_id, None, None, "deleted")
            .await
    }

    pub async fn move_accessible_event_to_deleted_items(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
        imported_identity: Option<MapiEventImportedMoveIdentity>,
    ) -> Result<MoveAccessibleEventToDeletedItemsResult> {
        let event = self
            .fetch_accessible_events_by_ids(principal_account_id, &[event_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("event not found"))?;
        if !event.rights.may_delete {
            bail!("delete access is not granted on this calendar");
        }

        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let locked = sqlx::query(
            r#"
            SELECT
                event.owner_account_id,
                event.calendar_id,
                event.uid,
                CASE
                    WHEN event.owner_account_id = $3 THEN TRUE
                    ELSE COALESCE(grant_row.may_delete, FALSE)
                END AS may_delete
            FROM calendar_events event
            LEFT JOIN calendar_grants grant_row
              ON grant_row.tenant_id = event.tenant_id
             AND grant_row.owner_account_id = event.owner_account_id
             AND grant_row.calendar_id = event.calendar_id
             AND grant_row.grantee_account_id = $3
            WHERE event.tenant_id = $1
              AND event.id = $2
              AND event.lifecycle_state = 'active'
            FOR UPDATE OF event
            "#,
        )
        .bind(&tenant_id)
        .bind(event_id)
        .bind(principal_account_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("event not found"))?;
        if !locked.get::<bool, _>("may_delete") {
            bail!("delete access is not granted on this calendar");
        }
        let owner_account_id = locked.get::<Uuid, _>("owner_account_id");
        let calendar_id = locked.get::<Uuid, _>("calendar_id");
        let event_uid = locked.get::<String, _>("uid");
        let affected_principals = Self::calendar_event_affected_principals_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            event_id,
        )
        .await?;

        self.insert_collaboration_move_tombstone_in_tx(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            owner_account_id,
            Some(calendar_id),
            "calendar_event",
            event_id,
            Some(&event_uid),
            &affected_principals,
        )
        .await?;
        let destination_modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                owner_account_id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        let deleted_at = sqlx::query_scalar::<_, String>(
            r#"
            UPDATE calendar_events
            SET lifecycle_state = 'deleted',
                deleted_at = clock_timestamp(),
                modseq = $4,
                updated_at = GREATEST(clock_timestamp(), updated_at + INTERVAL '1 microsecond')
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = $3
              AND lifecycle_state = 'active'
            RETURNING to_char(
                deleted_at AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(event_id)
        .bind(destination_modseq)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("event disappeared while moving it to Deleted Items"))?;
        sqlx::query(
            r#"
            UPDATE calendars
            SET sync_modseq = GREATEST(sync_modseq, $4),
                updated_at = NOW()
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(calendar_id)
        .bind(destination_modseq)
        .execute(&mut *tx)
        .await?;

        let identity_moves = rekey_active_event_identities_in_tx(
            &mut tx,
            &tenant_id,
            principal_account_id,
            event_id,
            imported_identity.as_ref(),
        )
        .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(owner_account_id),
            None,
            "deleted_calendar_event",
            event_id,
            "created",
            destination_modseq,
            &affected_principals,
            serde_json::json!({
                "sourceCollectionId": calendar_id,
                "objectUid": event_uid,
                "deletedAt": deleted_at,
            }),
        )
        .await?;
        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            owner_account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(MoveAccessibleEventToDeletedItemsResult {
            event,
            principal_identity: identity_moves
                .into_iter()
                .find(|identity| identity.account_id == principal_account_id),
        })
    }
}

async fn rekey_active_event_identities_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    principal_account_id: Uuid,
    event_id: Uuid,
    imported_identity: Option<&MapiEventImportedMoveIdentity>,
) -> Result<Vec<MapiEventIdentityMove>> {
    let identities = sqlx::query(
        r#"
        SELECT
            account_id,
            mapi_object_id,
            source_key,
            mapi_change_number,
            change_key,
            instance_key,
            predecessor_change_list
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND object_kind = 'calendar_event'
          AND canonical_id = $2
          AND deleted_at IS NULL
        ORDER BY account_id
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(event_id)
    .fetch_all(&mut **tx)
    .await?;
    let mut moves = Vec::with_capacity(identities.len());
    let mut imported_identity_applied = false;
    for identity in identities {
        let account_id = identity.get::<Uuid, _>("account_id");
        let old_mapi_object_id = checked_positive_u64(
            identity.get::<i64, _>("mapi_object_id"),
            "MAPI Event object id",
        )?;
        let old_source_key = identity.get::<Vec<u8>, _>("source_key");
        let old_change_number = checked_positive_u64(
            identity.get::<i64, _>("mapi_change_number"),
            "MAPI Event change number",
        )?;
        let old_change_key = identity.get::<Vec<u8>, _>("change_key");
        let old_instance_key = identity.get::<Vec<u8>, _>("instance_key");
        let predecessor_change_list = identity.get::<Vec<u8>, _>("predecessor_change_list");
        let principal_imported_identity =
            imported_identity.filter(|_| account_id == principal_account_id);
        let imported_destination_counter = principal_imported_identity
            .map(imported_move_destination_global_counter)
            .transpose()?;
        let minimum_change_number = imported_destination_counter
            .and_then(|counter| counter.checked_add(1))
            .unwrap_or(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER);
        let (replica_guid, allocated_change_number) =
            allocate_global_counter_in_tx(tx, tenant_id, account_id, minimum_change_number).await?;
        let (
            new_global_counter,
            new_mapi_object_id,
            new_source_key,
            new_change_key,
            new_instance_key,
            new_predecessor_change_list,
        ) = if let (Some(imported), Some(destination_counter)) =
            (principal_imported_identity, imported_destination_counter)
        {
            if old_source_key != imported.expected_source_key {
                bail!("active MAPI Event SourceKey changed before the imported move");
            }
            if imported.destination_source_key.get(..16) != Some(replica_guid.as_bytes().as_slice())
            {
                bail!("imported Event move destination must use the local mailbox replica GUID");
            }
            let normalized_predecessors = merge_predecessor_change_list(
                &imported.predecessor_change_list,
                &imported.change_key,
            )?;
            if normalized_predecessors != imported.predecessor_change_list {
                bail!("imported Event move PCL must canonically contain its ChangeKey");
            }
            imported_identity_applied = true;
            (
                destination_counter,
                mapi_store_id(destination_counter),
                imported.destination_source_key.clone(),
                imported.change_key.clone(),
                imported.destination_source_key.clone(),
                imported.predecessor_change_list.clone(),
            )
        } else {
            let new_source_key = mapi_change_key(replica_guid, allocated_change_number);
            let new_predecessor_change_list =
                merge_predecessor_change_list(&predecessor_change_list, &new_source_key)?;
            (
                allocated_change_number,
                mapi_store_id(allocated_change_number),
                new_source_key.clone(),
                new_source_key.clone(),
                new_source_key,
                new_predecessor_change_list,
            )
        };
        let new_change_number = allocated_change_number;

        sqlx::query(
            r#"
            INSERT INTO mapi_calendar_event_identity_moves (
                tenant_id,
                account_id,
                event_id,
                old_mapi_object_id,
                new_mapi_object_id,
                old_source_key,
                new_source_key,
                old_change_number,
                new_change_number,
                old_change_key,
                new_change_key,
                old_instance_key,
                new_instance_key,
                new_predecessor_change_list
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12, $13, $14
            )
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(event_id)
        .bind(old_mapi_object_id as i64)
        .bind(new_mapi_object_id as i64)
        .bind(&old_source_key)
        .bind(&new_source_key)
        .bind(old_change_number as i64)
        .bind(new_change_number as i64)
        .bind(&old_change_key)
        .bind(&new_change_key)
        .bind(&old_instance_key)
        .bind(&new_instance_key)
        .bind(&new_predecessor_change_list)
        .execute(&mut **tx)
        .await?;
        let updated = sqlx::query(
            r#"
            UPDATE mapi_object_identities
            SET object_kind = 'deleted_calendar_event',
                mapi_global_counter = $5,
                mapi_object_id = $6,
                source_key = $7,
                change_key = $8,
                instance_key = $7,
                mapi_change_number = $9,
                predecessor_change_list = $10,
                deleted_at = NULL,
                created_at = NOW(),
                updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = 'calendar_event'
              AND canonical_id = $3
              AND mapi_object_id = $4
              AND deleted_at IS NULL
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(event_id)
        .bind(old_mapi_object_id as i64)
        .bind(new_global_counter as i64)
        .bind(new_mapi_object_id as i64)
        .bind(&new_source_key)
        .bind(&new_change_key)
        .bind(new_change_number as i64)
        .bind(&new_predecessor_change_list)
        .execute(&mut **tx)
        .await?;
        if updated.rows_affected() != 1 {
            bail!("active MAPI Event identity disappeared during Deleted Items move");
        }
        moves.push(MapiEventIdentityMove {
            account_id,
            old_mapi_object_id,
            new_mapi_object_id,
            old_source_key,
            new_source_key,
            old_change_number,
            new_change_number,
            old_change_key,
            new_change_key,
        });
    }
    if imported_identity.is_some() && !imported_identity_applied {
        bail!("principal active MAPI Event identity was not found for the imported move");
    }
    Ok(moves)
}

fn imported_move_destination_global_counter(
    identity: &MapiEventImportedMoveIdentity,
) -> Result<u64> {
    if identity.expected_source_key.len() != 22 {
        bail!("imported Event move source GID must be exactly 22 bytes");
    }
    if identity.destination_source_key.len() != 22 {
        bail!("imported Event move destination GID must be exactly 22 bytes");
    }
    let mut counter_bytes = [0u8; 8];
    counter_bytes[2..].copy_from_slice(&identity.destination_source_key[16..]);
    let global_counter = u64::from_be_bytes(counter_bytes);
    if !(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER..FIRST_RESERVED_HIGH_GLOBAL_COUNTER)
        .contains(&global_counter)
    {
        bail!("imported Event move destination GLOBCNT is outside the dynamic local range");
    }
    Ok(global_counter)
}

async fn allocate_global_counter_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    account_id: Uuid,
    minimum_global_counter: u64,
) -> Result<(Uuid, u64)> {
    sqlx::query(
        r#"
        UPDATE mapi_mailbox_replicas replica
        SET next_global_counter = GREATEST(
                replica.next_global_counter,
                $3,
                COALESCE(
                    (
                        SELECT MAX(GREATEST(
                            identity.mapi_global_counter,
                            identity.mapi_change_number
                        )) + 1
                        FROM mapi_object_identities identity
                        WHERE identity.tenant_id = replica.tenant_id
                          AND identity.account_id = replica.account_id
                          AND identity.mapi_global_counter < $4
                          AND identity.mapi_change_number < $4
                    ),
                    $3
                )
            ),
            updated_at = NOW()
        WHERE replica.tenant_id = $1
          AND replica.account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(minimum_global_counter as i64)
    .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
    .execute(&mut **tx)
    .await?;
    let allocated = sqlx::query(
        r#"
        UPDATE mapi_mailbox_replicas
        SET next_global_counter = next_global_counter + 1,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
          AND next_global_counter >= $3
          AND next_global_counter < $4
        RETURNING replica_guid, next_global_counter - 1 AS global_counter
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(minimum_global_counter as i64)
    .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow!("MAPI dynamic global counter space exhausted"))?;
    let global_counter = checked_positive_u64(
        allocated.get::<i64, _>("global_counter"),
        "MAPI global counter",
    )?;
    if global_counter > MAX_MAPI_GLOBAL_COUNTER
        || global_counter >= FIRST_RESERVED_HIGH_GLOBAL_COUNTER
    {
        bail!("MAPI dynamic global counter space exhausted");
    }
    Ok((allocated.get::<Uuid, _>("replica_guid"), global_counter))
}

fn checked_positive_u64(value: i64, label: &str) -> Result<u64> {
    if value <= 0 {
        bail!("stored {label} is outside its valid positive range");
    }
    Ok(value as u64)
}
