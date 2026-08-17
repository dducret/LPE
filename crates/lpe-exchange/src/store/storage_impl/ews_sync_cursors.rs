macro_rules! store_impl_ews_sync_cursors {
    () => {
        fn fetch_ews_sync_cursor<'a>(
            &'a self,
            account_id: Uuid,
            cursor_id: Uuid,
        ) -> StoreFuture<'a, Option<EwsSyncCursor>> {
            Box::pin(async move {
                let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
                let row = sqlx::query(
                    r#"
                    SELECT scope, snapshot_json
                    FROM ews_sync_cursors
                    WHERE id = $1
                      AND tenant_id = $2
                      AND account_id = $3
                      AND expires_at > NOW()
                    "#,
                )
                .bind(cursor_id)
                .bind(tenant_id)
                .bind(account_id)
                .fetch_optional(self.pool())
                .await?;
                row.map(|row| {
                    Ok(EwsSyncCursor {
                        scope: row.try_get("scope")?,
                        snapshot_json: row.try_get("snapshot_json")?,
                    })
                })
                .transpose()
            })
        }

        fn store_ews_sync_cursor<'a>(
            &'a self,
            account_id: Uuid,
            scope: &'a str,
            snapshot_json: serde_json::Value,
        ) -> StoreFuture<'a, Uuid> {
            Box::pin(async move {
                if scope.is_empty() || scope.len() > 512 || !snapshot_json.is_object() {
                    anyhow::bail!("invalid EWS synchronization cursor");
                }
                let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
                let cursor_id = Uuid::new_v4();
                sqlx::query(
                    "DELETE FROM ews_sync_cursors WHERE tenant_id = $1 AND account_id = $2 AND expires_at <= NOW()",
                )
                .bind(tenant_id)
                .bind(account_id)
                .execute(self.pool())
                .await?;
                sqlx::query(
                    r#"
                    INSERT INTO ews_sync_cursors (
                        id, tenant_id, account_id, scope, snapshot_json, expires_at
                    )
                    VALUES ($1, $2, $3, $4, $5, NOW() + INTERVAL '30 days')
                    "#,
                )
                .bind(cursor_id)
                .bind(tenant_id)
                .bind(account_id)
                .bind(scope)
                .bind(snapshot_json)
                .execute(self.pool())
                .await?;
                Ok(cursor_id)
            })
        }
    };
}
