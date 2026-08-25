use anyhow::{anyhow, bail, Result};
use uuid::Uuid;

use crate::Storage;

use super::{
    types::{delegate_freebusy_message_objects, merge_free_busy_rows},
    DelegateFreeBusyMessageObject, FreeBusyBlock,
};

impl Storage {
    pub async fn fetch_free_busy_blocks(
        &self,
        principal_account_id: Uuid,
        owner_account_id: Uuid,
        starts_before: &str,
        ends_after: &str,
    ) -> Result<Vec<FreeBusyBlock>> {
        let principal_tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let owner = self
            .account_identity_for_id(owner_account_id)
            .await
            .map_err(|_| anyhow!("calendar owner not found"))?;
        let owner_tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        if principal_tenant_id != owner_tenant_id {
            bail!("free/busy is available only inside one tenant");
        }

        let can_read_details = if principal_account_id == owner_account_id {
            true
        } else {
            sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM calendar_grants grant_row
                    JOIN calendars calendar
                      ON calendar.tenant_id = grant_row.tenant_id
                     AND calendar.owner_account_id = grant_row.owner_account_id
                     AND calendar.id = grant_row.calendar_id
                     AND calendar.role = 'calendar'
                    WHERE grant_row.tenant_id = $1
                      AND grant_row.owner_account_id = $2
                      AND grant_row.grantee_account_id = $3
                      AND grant_row.may_read
                )
                "#,
            )
            .bind(&principal_tenant_id)
            .bind(owner_account_id)
            .bind(principal_account_id)
            .fetch_one(&self.pool)
            .await?
        };

        let rows = sqlx::query_as::<_, crate::FreeBusyEventRow>(
            r#"
            SELECT
                to_char(GREATEST(e.starts_at, $4::timestamptz) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS starts_at,
                to_char(LEAST(e.ends_at, $3::timestamptz) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS ends_at,
                e.status
            FROM calendar_events e
            JOIN calendars c
              ON c.tenant_id = e.tenant_id
             AND c.owner_account_id = e.owner_account_id
             AND c.id = e.calendar_id
             AND c.role = 'calendar'
            WHERE e.tenant_id = $1
              AND e.owner_account_id = $2
              AND e.lifecycle_state = 'active'
              AND e.projection_state = 'visible'
              AND e.status <> 'cancelled'
              AND e.starts_at < $3::timestamptz
              AND e.ends_at > $4::timestamptz
            ORDER BY e.starts_at ASC, e.ends_at ASC, e.id ASC
            "#,
        )
        .bind(&principal_tenant_id)
        .bind(owner_account_id)
        .bind(starts_before)
        .bind(ends_after)
        .fetch_all(&self.pool)
        .await?;

        Ok(merge_free_busy_rows(
            rows,
            owner_account_id,
            owner.email,
            can_read_details,
        ))
    }

    pub async fn project_delegate_freebusy_messages(
        &self,
        principal_account_id: Uuid,
        owner_account_id: Uuid,
        starts_before: &str,
        ends_after: &str,
    ) -> Result<Vec<DelegateFreeBusyMessageObject>> {
        self.compute_delegate_freebusy_messages(
            principal_account_id,
            Some(owner_account_id),
            starts_before,
            ends_after,
        )
        .await
    }

    pub async fn fetch_delegate_freebusy_messages(
        &self,
        principal_account_id: Uuid,
        owner_account_id: Option<Uuid>,
    ) -> Result<Vec<DelegateFreeBusyMessageObject>> {
        self.compute_delegate_freebusy_messages(
            principal_account_id,
            owner_account_id,
            "9999-12-31T23:59:59Z",
            "1970-01-01T00:00:00Z",
        )
        .await
    }

    async fn compute_delegate_freebusy_messages(
        &self,
        principal_account_id: Uuid,
        owner_account_id: Option<Uuid>,
        starts_before: &str,
        ends_after: &str,
    ) -> Result<Vec<DelegateFreeBusyMessageObject>> {
        let delegate_objects = self
            .fetch_delegate_access_objects(principal_account_id)
            .await?;
        let mut messages = Vec::new();
        if let Some(owner_account_id) = owner_account_id {
            let delegate = delegate_objects
                .iter()
                .find(|object| object.owner_account_id == owner_account_id);
            let free_busy = self
                .fetch_free_busy_blocks(
                    principal_account_id,
                    owner_account_id,
                    starts_before,
                    ends_after,
                )
                .await?;
            messages.extend(delegate_freebusy_message_objects(
                principal_account_id,
                owner_account_id,
                delegate,
                free_busy,
            )?);
        } else {
            for delegate in &delegate_objects {
                let free_busy = self
                    .fetch_free_busy_blocks(
                        principal_account_id,
                        delegate.owner_account_id,
                        starts_before,
                        ends_after,
                    )
                    .await?;
                messages.extend(delegate_freebusy_message_objects(
                    principal_account_id,
                    delegate.owner_account_id,
                    Some(delegate),
                    free_busy,
                )?);
            }
        }
        messages.sort_by(|left, right| {
            left.owner_account_id
                .cmp(&right.owner_account_id)
                .then(left.message_kind.cmp(&right.message_kind))
                .then(left.starts_at.cmp(&right.starts_at))
                .then(left.id.cmp(&right.id))
        });
        Ok(messages)
    }
}
