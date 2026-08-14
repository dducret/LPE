macro_rules! store_impl_mapi_sync_checkpoints {
    () => {
    fn fetch_mapi_sync_checkpoint<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
    ) -> StoreFuture<'a, Option<MapiSyncCheckpoint>> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let store_identity = Storage::fetch_mapi_store_identity(self).await?;
            let row = sqlx::query(
                r#"
                SELECT mailbox_id, checkpoint_kind, last_change_sequence, last_modseq, cursor_json
                FROM mapi_sync_checkpoints
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND checkpoint_kind = $3
                  AND mapi_replica_guid = $4
                  AND expires_at > NOW()
                  AND (
                      ($5::uuid IS NULL AND mailbox_id IS NULL)
                      OR mailbox_id = $5
                  )
                LIMIT 1
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(checkpoint_kind.as_str())
            .bind(store_identity.replica_guid)
            .bind(mailbox_id)
            .fetch_optional(self.pool())
            .await?;

            row.map(mapi_sync_checkpoint_from_row).transpose()
        })
    }

    fn store_mapi_sync_checkpoint<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
        last_change_sequence: u64,
        last_modseq: u64,
        cursor_json: serde_json::Value,
    ) -> StoreFuture<'a, MapiSyncCheckpoint> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let mut tx = self.pool().begin().await?;
            let store_identity =
                mapi_store_identity_for_account_in_tx(&mut tx, tenant_id, account_id).await?;
            let existing = sqlx::query(
                r#"
                SELECT id, mailbox_id, checkpoint_kind, last_change_sequence, last_modseq, cursor_json,
                       expires_at > NOW() AS checkpoint_is_live
                FROM mapi_sync_checkpoints
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND checkpoint_kind = $3
                  AND mapi_replica_guid = $4
                  AND (
                      ($5::uuid IS NULL AND mailbox_id IS NULL)
                      OR mailbox_id = $5
                  )
                LIMIT 1
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(checkpoint_kind.as_str())
            .bind(store_identity.replica_guid)
            .bind(mailbox_id)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(existing) = existing.as_ref() {
                let existing_change_sequence =
                    existing.get::<i64, _>("last_change_sequence").max(0) as u64;
                let existing_modseq = existing.get::<i64, _>("last_modseq").max(0) as u64;
                let checkpoint_is_live = existing.get::<bool, _>("checkpoint_is_live");
                if checkpoint_is_live
                    && (existing_change_sequence > last_change_sequence
                    || (existing_change_sequence == last_change_sequence
                        && existing_modseq > last_modseq))
                {
                    let checkpoint = MapiSyncCheckpoint {
                        mailbox_id: existing.get::<Option<Uuid>, _>("mailbox_id"),
                        checkpoint_kind,
                        last_change_sequence: existing_change_sequence,
                        last_modseq: existing_modseq,
                        cursor_json: existing.get("cursor_json"),
                    };
                    tx.commit().await?;
                    return Ok(checkpoint);
                }
            }
            let existing_id = existing.as_ref().map(|row| row.get::<Uuid, _>("id"));
            let row = sqlx::query(
                if existing_id.is_some() {
                    r#"
                    UPDATE mapi_sync_checkpoints
                    SET
                        last_change_sequence = $7,
                        last_modseq = $8,
                        cursor_json = $9,
                        updated_at = NOW(),
                        expires_at = NOW() + INTERVAL '30 days'
                    WHERE id = $1
                    RETURNING mailbox_id, checkpoint_kind, last_change_sequence, last_modseq, cursor_json
                    "#
                } else {
                    r#"
                    INSERT INTO mapi_sync_checkpoints (
                        id, tenant_id, account_id, mailbox_id, checkpoint_kind,
                        mapi_replica_guid, last_change_sequence, last_modseq,
                        cursor_json, expires_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW() + INTERVAL '30 days')
                    RETURNING mailbox_id, checkpoint_kind, last_change_sequence, last_modseq, cursor_json
                    "#
                },
            )
            .bind(existing_id.unwrap_or_else(Uuid::new_v4))
            .bind(&tenant_id)
            .bind(account_id)
            .bind(mailbox_id)
            .bind(checkpoint_kind.as_str())
            .bind(store_identity.replica_guid)
            .bind(last_change_sequence as i64)
            .bind(last_modseq as i64)
            .bind(cursor_json)
            .fetch_one(&mut *tx)
            .await?;
            tx.commit().await?;

            mapi_sync_checkpoint_from_row(row)
        })
    }

    fn fetch_mapi_ipm_subtree_ost_id<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move { Storage::fetch_mapi_ipm_subtree_ost_id(self, account_id).await })
    }

    fn store_mapi_ipm_subtree_ost_id<'a>(
        &'a self,
        account_id: Uuid,
        ost_id: &'a [u8],
    ) -> StoreFuture<'a, ()> {
        Box::pin(
            async move { Storage::store_mapi_ipm_subtree_ost_id(self, account_id, ost_id).await },
        )
    }

    };
}
