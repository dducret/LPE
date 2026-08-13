use anyhow::{bail, Result};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{AuditEntryInput, Storage};

use super::{DelegatePreferencesPatch, MailboxDelegationGrant, MailboxDelegationGrantInput};

impl Storage {
    pub async fn upsert_mailbox_delegation_grant_with_preferences(
        &self,
        input: MailboxDelegationGrantInput,
        delegate_preferences: DelegatePreferencesPatch,
        audit: AuditEntryInput,
    ) -> Result<MailboxDelegationGrant> {
        self.upsert_mailbox_delegation_grant_inner(input, delegate_preferences, audit)
            .await
    }
}

pub(super) fn validate_delegate_preferences_patch(
    preferences: &DelegatePreferencesPatch,
) -> Result<()> {
    if let Some(delivery) = preferences.meeting_request_delivery.as_deref() {
        if !matches!(
            delivery,
            "delegate_only" | "delegate_and_owner" | "owner_only"
        ) {
            bail!("unsupported meeting request delivery");
        }
    }
    Ok(())
}

pub(super) async fn upsert_delegate_preferences_patch_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    grantee_account_id: Uuid,
    preferences: &DelegatePreferencesPatch,
) -> Result<()> {
    if preferences.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO delegate_preferences (
            tenant_id, owner_account_id, grantee_account_id,
            meeting_request_delivery, receives_meeting_request_copy,
            may_view_private_items
        )
        VALUES (
            $1, $2, $3,
            COALESCE($4::TEXT, 'delegate_and_owner'),
            COALESCE($5::BOOLEAN, TRUE),
            COALESCE($6::BOOLEAN, FALSE)
        )
        ON CONFLICT (tenant_id, owner_account_id, grantee_account_id)
        DO UPDATE SET
            meeting_request_delivery = COALESCE(
                $4::TEXT,
                delegate_preferences.meeting_request_delivery
            ),
            receives_meeting_request_copy = COALESCE(
                $5::BOOLEAN,
                delegate_preferences.receives_meeting_request_copy
            ),
            may_view_private_items = COALESCE(
                $6::BOOLEAN,
                delegate_preferences.may_view_private_items
            ),
            updated_at = NOW()
        "#,
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(grantee_account_id)
    .bind(preferences.meeting_request_delivery.as_deref())
    .bind(preferences.receives_meeting_request_copy)
    .bind(preferences.may_view_private_items)
    .execute(&mut **tx)
    .await?;

    Ok(())
}
