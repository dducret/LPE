use anyhow::Result;
use uuid::Uuid;

use crate::{CanonicalChangeCategory, Storage};

use super::PublicFolderAccess;

impl Storage {
    pub(super) async fn record_public_folder_change(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        access: &PublicFolderAccess,
        actor_account_id: Uuid,
        folder_id: Uuid,
        object_kind: &str,
        object_id: Uuid,
        change_kind: &str,
        summary_json: serde_json::Value,
    ) -> Result<i64> {
        self.record_public_folder_change_with_extra_affected(
            tx,
            access,
            actor_account_id,
            folder_id,
            object_kind,
            object_id,
            change_kind,
            summary_json,
            &[],
        )
        .await
    }

    pub(super) async fn record_public_folder_change_with_extra_affected(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        access: &PublicFolderAccess,
        actor_account_id: Uuid,
        folder_id: Uuid,
        object_kind: &str,
        object_id: Uuid,
        change_kind: &str,
        summary_json: serde_json::Value,
        extra_affected_account_ids: &[Uuid],
    ) -> Result<i64> {
        let mut affected = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT principal_account_id
            FROM public_folder_permissions
            WHERE tenant_id = $1 AND public_folder_id = $2 AND may_read
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .fetch_all(&mut **tx)
        .await?;
        affected.push(access.tree_admin_owner_account_id);
        affected.push(actor_account_id);
        affected.extend(extra_affected_account_ids);
        affected.sort();
        affected.dedup();
        let modseq = self
            .allocate_account_modseq_in_tx(
                tx,
                &access.tenant_id,
                access.tree_admin_owner_account_id,
                CanonicalChangeCategory::PublicFolders.as_str(),
            )
            .await?;
        let cursor = Self::insert_mail_change_log_in_tx(
            tx,
            &access.tenant_id,
            Some(access.tree_admin_owner_account_id),
            None,
            object_kind,
            object_id,
            change_kind,
            modseq,
            &affected,
            summary_json,
        )
        .await?;
        Self::emit_canonical_change(
            tx,
            &access.tenant_id,
            CanonicalChangeCategory::PublicFolders,
            &affected,
            &affected,
        )
        .await?;
        Ok(cursor)
    }

    pub(super) async fn record_public_folder_private_change(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        access: &PublicFolderAccess,
        actor_account_id: Uuid,
        object_kind: &str,
        object_id: Uuid,
        change_kind: &str,
        summary_json: serde_json::Value,
    ) -> Result<i64> {
        let modseq = self
            .allocate_account_modseq_in_tx(
                tx,
                &access.tenant_id,
                actor_account_id,
                CanonicalChangeCategory::PublicFolders.as_str(),
            )
            .await?;
        let cursor = Self::insert_mail_change_log_in_tx(
            tx,
            &access.tenant_id,
            Some(actor_account_id),
            None,
            object_kind,
            object_id,
            change_kind,
            modseq,
            &[actor_account_id],
            summary_json,
        )
        .await?;
        Self::emit_canonical_change(
            tx,
            &access.tenant_id,
            CanonicalChangeCategory::PublicFolders,
            &[actor_account_id],
            &[actor_account_id],
        )
        .await?;
        Ok(cursor)
    }
}
