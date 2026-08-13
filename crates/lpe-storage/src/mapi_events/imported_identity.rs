use anyhow::{anyhow, bail, Result};
use sqlx::Postgres;
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
    }
    let (_, change_number) = allocate_mapi_store_global_counter_in_tx(tx).await?;

    // [MS-OXCFXICS] 3.1.5.3 assigns the imported version its client CK. The
    // subsequent normal-Message Save observed from Exchange advances that
    // version again: keep the imported SourceKey/MID, return a server CK that
    // matches the allocated CN, and retain the imported CK in the PCL.
    let (source_global_counter, source_key, change_key, predecessor_change_list) =
        match (imported_source_counter, imported_identity) {
            (Some(source_global_counter), Some(identity)) => {
                if source_global_counter == change_number {
                    bail!("MAPI Event imported SourceKey and server ChangeNumber must differ");
                }
                let change_key = mapi_change_key(replica_guid, change_number);
                let predecessor_change_list =
                    merge_predecessor_change_list(&identity.predecessor_change_list, &change_key)?;
                (
                    source_global_counter,
                    identity.source_key.clone(),
                    change_key,
                    predecessor_change_list,
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
            source_key: mapi_change_key(Uuid::from_u128(2), 0x0df8_974b_7f66),
            change_key,
            predecessor_change_list,
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
