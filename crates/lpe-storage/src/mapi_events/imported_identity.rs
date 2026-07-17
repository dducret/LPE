use anyhow::{anyhow, bail, Result};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use super::{
    mapi_change_key, mapi_store_id, merge_predecessor_change_list, EventIdentityVersion,
    MapiEventImportedIdentity, FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER,
    FIRST_RESERVED_HIGH_GLOBAL_COUNTER, MAPI_STORE_REPLICA_GUID, MAX_MAPI_GLOBAL_COUNTER,
};

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
    Ok(())
}

fn imported_source_global_counter(
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
    sqlx::query(
        r#"
        INSERT INTO mapi_mailbox_replicas (
            tenant_id, account_id, replica_guid, next_global_counter
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tenant_id, account_id)
        DO UPDATE SET
            next_global_counter = GREATEST(
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
    .bind(principal_account_id)
    .fetch_one(&mut **tx)
    .await?;
    let imported_source_counter = imported_identity
        .map(|identity| imported_source_global_counter(identity, replica_guid))
        .transpose()?;
    let allocation_floor = imported_source_counter
        .map(|counter| counter + 1)
        .unwrap_or(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER);
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
        WHERE replica.tenant_id = $1
          AND replica.account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
    .bind(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER as i64)
    .bind(allocation_floor as i64)
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
        RETURNING next_global_counter - 1 AS change_number
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER as i64)
    .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow!("MAPI dynamic global counter space exhausted"))?;
    let change_number = allocated.get::<i64, _>("change_number");
    if change_number <= 0 || change_number as u64 > MAX_MAPI_GLOBAL_COUNTER {
        bail!("MAPI dynamic global counter space exhausted");
    }
    let change_number = change_number as u64;

    // [MS-OXCFXICS] 3.1.5.3: an imported message keeps the client SourceKey,
    // ChangeKey and PCL{ChangeKey}, but receives a server-internal CN.
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
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id,
            mapi_global_counter, mapi_object_id, source_key, change_key,
            instance_key, mapi_change_number, predecessor_change_list
        )
        VALUES ($1, $2, 'calendar_event', $3, $4, $5, $6, $7, $6, $8, $9)
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
    .execute(&mut **tx)
    .await?;
    Ok((
        object_id,
        EventIdentityVersion {
            account_id: principal_account_id,
            change_number,
            change_key,
            predecessor_change_list,
        },
    ))
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
            source_key: mapi_change_key(
                Uuid::from_bytes(MAPI_STORE_REPLICA_GUID),
                0x0df8_974b_7f66,
            ),
            change_key,
            predecessor_change_list,
        }
    }

    #[test]
    fn imported_identity_accepts_a_canonical_multi_replica_pcl_containing_its_change_key() {
        let mut identity = realistic_imported_identity();
        assert!(validate_imported_identity(&identity).is_ok());
        assert_eq!(
            imported_source_global_counter(&identity, Uuid::from_bytes(MAPI_STORE_REPLICA_GUID))
                .unwrap(),
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
