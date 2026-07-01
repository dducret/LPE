use anyhow::{anyhow, bail, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    normalize_email, AuditEntryInput, MailboxAccountAccessRow, MailboxDelegationGrantRow,
    SenderDelegationGrantRow, Storage,
};

use super::types::{
    map_mailbox_delegation_grant, map_sender_delegation_grant, sender_identity_id,
    validate_mailbox_delegation_rights, MailboxAccountAccess, MailboxDelegationGrant,
    MailboxDelegationGrantInput, MailboxFolderDelegationGrantInput, SenderAuthorizationKind,
    SenderDelegationGrant, SenderDelegationGrantInput, SenderDelegationRight, SenderIdentity,
};

impl Storage {
    pub async fn fetch_account_identity(&self, account_id: Uuid) -> Result<MailboxAccountAccess> {
        let account = self.account_identity_for_id(account_id).await?;
        Ok(MailboxAccountAccess {
            tenant_id: self.tenant_id_for_account_id(account.id).await?,
            account_id: account.id,
            email: account.email,
            display_name: account.display_name,
            is_owned: true,
            may_read: true,
            may_write: true,
            may_send_as: true,
            may_send_on_behalf: false,
        })
    }

    pub async fn upsert_mailbox_delegation_grant(
        &self,
        input: MailboxDelegationGrantInput,
        audit: AuditEntryInput,
    ) -> Result<MailboxDelegationGrant> {
        let tenant_id = self
            .tenant_id_for_account_id(input.owner_account_id)
            .await?;
        let grantee_email = normalize_email(&input.grantee_email);
        if grantee_email.is_empty() {
            bail!("grantee email is required");
        }
        let mailbox_id = self
            .default_mailbox_delegation_mailbox_id(input.owner_account_id)
            .await?;

        let mut tx = self.pool.begin().await?;
        let owner = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, input.owner_account_id)
            .await?;
        let grantee = self
            .load_account_identity_by_email_in_tx(&mut tx, &tenant_id, &grantee_email)
            .await?;

        if owner.id == grantee.id {
            bail!("self-delegation is not supported");
        }

        let grant_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO mailbox_delegation_grants (
                id, tenant_id, mailbox_id, owner_account_id, grantee_account_id,
                may_read, may_write, may_delete, may_share
            )
            VALUES ($1, $2, $3, $4, $5, TRUE, $6, FALSE, FALSE)
            ON CONFLICT (tenant_id, mailbox_id, grantee_account_id)
            DO UPDATE SET
                may_read = TRUE,
                may_write = EXCLUDED.may_write,
                may_delete = FALSE,
                may_share = FALSE,
                updated_at = NOW()
            RETURNING id
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(mailbox_id)
        .bind(owner.id)
        .bind(grantee.id)
        .bind(input.may_write)
        .fetch_one(&mut *tx)
        .await?;

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, owner.id)
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(owner.id),
            None,
            "mailbox_delegation_grant",
            grant_id,
            "updated",
            modseq,
            &[owner.id, grantee.id],
            serde_json::json!({
                "mailboxId": mailbox_id,
                "granteeId": grantee.id,
                "mayWrite": input.may_write
            }),
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(&mut tx, &tenant_id, owner.id, grantee.id).await?;
        tx.commit().await?;

        self.fetch_mailbox_delegation_grant(owner.id, grantee.id)
            .await?
            .ok_or_else(|| anyhow!("mailbox delegation grant not found after upsert"))
    }

    pub async fn set_mailbox_folder_delegation_grant(
        &self,
        input: MailboxFolderDelegationGrantInput,
        audit: AuditEntryInput,
    ) -> Result<()> {
        validate_mailbox_delegation_rights(
            input.may_read,
            input.may_write,
            input.may_delete,
            input.may_share,
        )?;
        let tenant_id = self
            .tenant_id_for_account_id(input.owner_account_id)
            .await?;
        let mut tx = self.pool.begin().await?;
        let owner = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, input.owner_account_id)
            .await?;
        let grantee = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, input.grantee_account_id)
            .await?;

        if owner.id == grantee.id {
            bail!("self-delegation is not supported");
        }

        let mailbox_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM mailboxes
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND id = $3
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(owner.id)
        .bind(input.mailbox_id)
        .fetch_one(&mut *tx)
        .await?;
        if !mailbox_exists {
            bail!("mailbox not found for owner account");
        }

        if input.may_read {
            let grant_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                INSERT INTO mailbox_delegation_grants (
                    id, tenant_id, mailbox_id, owner_account_id, grantee_account_id,
                    may_read, may_write, may_delete, may_share
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (tenant_id, mailbox_id, grantee_account_id)
                DO UPDATE SET
                    may_read = EXCLUDED.may_read,
                    may_write = EXCLUDED.may_write,
                    may_delete = EXCLUDED.may_delete,
                    may_share = EXCLUDED.may_share,
                    updated_at = NOW()
                RETURNING id
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(input.mailbox_id)
            .bind(owner.id)
            .bind(grantee.id)
            .bind(input.may_read)
            .bind(input.may_write)
            .bind(input.may_delete)
            .bind(input.may_share)
            .fetch_one(&mut *tx)
            .await?;

            let modseq = self
                .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, owner.id)
                .await?;
            Self::insert_mail_change_log_in_tx(
                &mut tx,
                &tenant_id,
                Some(owner.id),
                Some(input.mailbox_id),
                "mailbox_delegation_grant",
                grant_id,
                "updated",
                modseq,
                &[owner.id, grantee.id],
                serde_json::json!({
                    "mailboxId": input.mailbox_id,
                    "granteeId": grantee.id,
                    "mayRead": input.may_read,
                    "mayWrite": input.may_write,
                    "mayDelete": input.may_delete,
                    "mayShare": input.may_share
                }),
            )
            .await?;
        } else {
            let grant_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                DELETE FROM mailbox_delegation_grants
                WHERE tenant_id = $1
                  AND mailbox_id = $2
                  AND owner_account_id = $3
                  AND grantee_account_id = $4
                RETURNING id
                "#,
            )
            .bind(&tenant_id)
            .bind(input.mailbox_id)
            .bind(owner.id)
            .bind(grantee.id)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(grant_id) = grant_id {
                let modseq = self
                    .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, owner.id)
                    .await?;
                Self::insert_mail_change_log_in_tx(
                    &mut tx,
                    &tenant_id,
                    Some(owner.id),
                    Some(input.mailbox_id),
                    "mailbox_delegation_grant",
                    grant_id,
                    "destroyed",
                    modseq,
                    &[owner.id, grantee.id],
                    serde_json::json!({
                        "mailboxId": input.mailbox_id,
                        "granteeId": grantee.id
                    }),
                )
                .await?;
            }
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(&mut tx, &tenant_id, owner.id, grantee.id).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn delete_mailbox_delegation_grant(
        &self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted_rows = sqlx::query(
            r#"
            DELETE FROM mailbox_delegation_grants
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND grantee_account_id = $3
            RETURNING id, mailbox_id
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .fetch_all(&mut *tx)
        .await?;

        if deleted_rows.is_empty() {
            bail!("mailbox delegation grant not found");
        }

        for row in deleted_rows {
            let grant_id: Uuid = row.try_get("id")?;
            let mailbox_id: Uuid = row.try_get("mailbox_id")?;
            let modseq = self
                .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, owner_account_id)
                .await?;
            Self::insert_mail_change_log_in_tx(
                &mut tx,
                &tenant_id,
                Some(owner_account_id),
                Some(mailbox_id),
                "mailbox_delegation_grant",
                grant_id,
                "destroyed",
                modseq,
                &[owner_account_id, grantee_account_id],
                serde_json::json!({
                    "mailboxId": mailbox_id,
                    "granteeId": grantee_account_id
                }),
            )
            .await?;
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(
            &mut tx,
            &tenant_id,
            owner_account_id,
            grantee_account_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn default_mailbox_delegation_mailbox_id(
        &self,
        owner_account_id: Uuid,
    ) -> Result<Uuid> {
        self.ensure_imap_mailboxes(owner_account_id)
            .await?
            .into_iter()
            .find(|mailbox| mailbox.role == "inbox")
            .map(|mailbox| mailbox.id)
            .ok_or_else(|| anyhow!("default mailbox not found"))
    }

    pub async fn fetch_mailbox_delegation_grant(
        &self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
    ) -> Result<Option<MailboxDelegationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let row = sqlx::query_as::<_, MailboxDelegationGrantRow>(
            r#"
            SELECT
                g.id,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.may_write,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM mailbox_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
              AND g.grantee_account_id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_mailbox_delegation_grant))
    }

    pub async fn upsert_sender_delegation_grant(
        &self,
        input: SenderDelegationGrantInput,
        audit: AuditEntryInput,
    ) -> Result<SenderDelegationGrant> {
        let tenant_id = self
            .tenant_id_for_account_id(input.owner_account_id)
            .await?;
        let grantee_email = normalize_email(&input.grantee_email);
        if grantee_email.is_empty() {
            bail!("grantee email is required");
        }

        let mut tx = self.pool.begin().await?;
        let owner = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, input.owner_account_id)
            .await?;
        let grantee = self
            .load_account_identity_by_email_in_tx(&mut tx, &tenant_id, &grantee_email)
            .await?;

        if owner.id == grantee.id {
            bail!("self-delegation is not supported");
        }

        let grant_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO sender_rights (
                id, tenant_id, owner_account_id, grantee_account_id, sender_right
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (tenant_id, owner_account_id, grantee_account_id, sender_right)
            WHERE identity_id IS NULL
            DO UPDATE SET updated_at = NOW()
            RETURNING id
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(owner.id)
        .bind(grantee.id)
        .bind(input.sender_right.as_str())
        .fetch_one(&mut *tx)
        .await?;

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, owner.id)
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(owner.id),
            None,
            "sender_right",
            grant_id,
            "updated",
            modseq,
            &[owner.id, grantee.id],
            serde_json::json!({
                "granteeId": grantee.id,
                "senderRight": input.sender_right.as_str()
            }),
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(&mut tx, &tenant_id, owner.id, grantee.id).await?;
        tx.commit().await?;

        self.fetch_sender_delegation_grant(owner.id, grantee.id, input.sender_right)
            .await?
            .ok_or_else(|| anyhow!("sender delegation grant not found after upsert"))
    }

    pub async fn delete_sender_delegation_grant(
        &self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        sender_right: SenderDelegationRight,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let grant_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            DELETE FROM sender_rights
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND grantee_account_id = $3
              AND sender_right = $4
              AND identity_id IS NULL
            RETURNING id
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .bind(sender_right.as_str())
        .fetch_optional(&mut *tx)
        .await?;

        let Some(grant_id) = grant_id else {
            bail!("sender delegation grant not found");
        };

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, owner_account_id)
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(owner_account_id),
            None,
            "sender_right",
            grant_id,
            "destroyed",
            modseq,
            &[owner_account_id, grantee_account_id],
            serde_json::json!({
                "granteeId": grantee_account_id,
                "senderRight": sender_right.as_str()
            }),
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(
            &mut tx,
            &tenant_id,
            owner_account_id,
            grantee_account_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_sender_delegation_grant(
        &self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        sender_right: SenderDelegationRight,
    ) -> Result<Option<SenderDelegationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let row = sqlx::query_as::<_, SenderDelegationGrantRow>(
            r#"
            SELECT
                g.id,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.sender_right,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sender_rights g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
              AND g.grantee_account_id = $3
              AND g.sender_right = $4
              AND g.identity_id IS NULL
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .bind(sender_right.as_str())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_sender_delegation_grant))
    }

    pub async fn fetch_outgoing_mailbox_delegation_grants(
        &self,
        owner_account_id: Uuid,
    ) -> Result<Vec<MailboxDelegationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let rows = sqlx::query_as::<_, MailboxDelegationGrantRow>(
            r#"
            SELECT
                g.id,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.may_write,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM mailbox_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
            ORDER BY lower(grantee.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_mailbox_delegation_grant).collect())
    }

    pub async fn fetch_outgoing_sender_delegation_grants(
        &self,
        owner_account_id: Uuid,
    ) -> Result<Vec<SenderDelegationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let rows = sqlx::query_as::<_, SenderDelegationGrantRow>(
            r#"
            SELECT
                g.id,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.sender_right,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sender_rights g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
              AND g.identity_id IS NULL
            ORDER BY lower(grantee.primary_email) ASC, g.sender_right ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_sender_delegation_grant).collect())
    }

    pub async fn fetch_accessible_mailbox_accounts(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<MailboxAccountAccess>> {
        let principal = self.account_identity_for_id(principal_account_id).await?;
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let mut accounts = vec![MailboxAccountAccess {
            tenant_id,
            account_id: principal.id,
            email: principal.email,
            display_name: principal.display_name,
            is_owned: true,
            may_read: true,
            may_write: true,
            may_send_as: true,
            may_send_on_behalf: false,
        }];

        let rows = sqlx::query_as::<_, MailboxAccountAccessRow>(
            r#"
            SELECT
                owner.id AS account_id,
                owner.primary_email AS email,
                owner.display_name,
                g.may_write,
                EXISTS(
                    SELECT 1
                    FROM sender_rights sg
                    WHERE sg.tenant_id = g.tenant_id
                      AND sg.owner_account_id = g.owner_account_id
                      AND sg.grantee_account_id = g.grantee_account_id
                      AND sg.sender_right = 'send_as'
                      AND sg.identity_id IS NULL
                ) AS may_send_as,
                EXISTS(
                    SELECT 1
                    FROM sender_rights sg
                    WHERE sg.tenant_id = g.tenant_id
                      AND sg.owner_account_id = g.owner_account_id
                      AND sg.grantee_account_id = g.grantee_account_id
                      AND sg.sender_right = 'send_on_behalf'
                      AND sg.identity_id IS NULL
                ) AS may_send_on_behalf
            FROM mailbox_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            WHERE g.tenant_id = $1
              AND g.grantee_account_id = $2
            ORDER BY lower(owner.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(principal_account_id)
        .fetch_all(&self.pool)
        .await?;

        accounts.extend(rows.into_iter().map(|row| MailboxAccountAccess {
            tenant_id,
            account_id: row.account_id,
            email: row.email,
            display_name: row.display_name,
            is_owned: false,
            may_read: true,
            may_write: row.may_write,
            may_send_as: row.may_send_as,
            may_send_on_behalf: row.may_send_on_behalf,
        }));
        Ok(accounts)
    }

    pub async fn require_mailbox_account_access(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<MailboxAccountAccess> {
        self.fetch_accessible_mailbox_accounts(principal_account_id)
            .await?
            .into_iter()
            .find(|account| account.account_id == target_account_id)
            .ok_or_else(|| anyhow!("mailbox account is not accessible"))
    }

    pub async fn fetch_sender_identities(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<Vec<SenderIdentity>> {
        let tenant_id = self.tenant_id_for_account_id(target_account_id).await?;
        let principal = self.account_identity_for_id(principal_account_id).await?;
        let target = self.account_identity_for_id(target_account_id).await?;
        let rights = sqlx::query(
            r#"
            SELECT
                EXISTS(
                    SELECT 1
                    FROM sender_rights
                    WHERE tenant_id = $1
                      AND owner_account_id = $2
                      AND grantee_account_id = $3
                      AND sender_right = 'send_as'
                      AND identity_id IS NULL
                ) AS may_send_as,
                EXISTS(
                    SELECT 1
                    FROM sender_rights
                    WHERE tenant_id = $1
                      AND owner_account_id = $2
                      AND grantee_account_id = $3
                      AND sender_right = 'send_on_behalf'
                      AND identity_id IS NULL
                ) AS may_send_on_behalf
            "#,
        )
        .bind(&tenant_id)
        .bind(target_account_id)
        .bind(principal_account_id)
        .fetch_one(&self.pool)
        .await?;
        let may_send_as: bool = rights.try_get("may_send_as")?;
        let may_send_on_behalf: bool = rights.try_get("may_send_on_behalf")?;

        let access =
            if principal_account_id == target_account_id || (!may_send_as && !may_send_on_behalf) {
                Some(
                    self.require_mailbox_account_access(principal_account_id, target_account_id)
                        .await?,
                )
            } else {
                self.require_mailbox_account_access(principal_account_id, target_account_id)
                    .await
                    .ok()
            };
        let is_owned = principal_account_id == target_account_id
            || access.as_ref().is_some_and(|access| access.is_owned);
        let email = access
            .as_ref()
            .map(|access| access.email.clone())
            .unwrap_or_else(|| target.email.clone());
        let display_name = access
            .as_ref()
            .map(|access| access.display_name.clone())
            .unwrap_or_else(|| target.display_name.clone());

        let mut identities = Vec::new();
        if is_owned {
            identities.push(SenderIdentity {
                id: sender_identity_id(SenderAuthorizationKind::SelfSend, target_account_id),
                owner_account_id: target_account_id,
                email: email.clone(),
                display_name: display_name.clone(),
                authorization_kind: SenderAuthorizationKind::SelfSend.as_str().to_string(),
                sender_address: None,
                sender_display: None,
            });
        } else {
            if may_send_as || access.as_ref().is_some_and(|access| access.may_send_as) {
                identities.push(SenderIdentity {
                    id: sender_identity_id(SenderAuthorizationKind::SendAs, target_account_id),
                    owner_account_id: target_account_id,
                    email: email.clone(),
                    display_name: display_name.clone(),
                    authorization_kind: SenderAuthorizationKind::SendAs.as_str().to_string(),
                    sender_address: None,
                    sender_display: None,
                });
            }
            if may_send_on_behalf
                || access
                    .as_ref()
                    .is_some_and(|access| access.may_send_on_behalf)
            {
                identities.push(SenderIdentity {
                    id: sender_identity_id(
                        SenderAuthorizationKind::SendOnBehalf,
                        target_account_id,
                    ),
                    owner_account_id: target_account_id,
                    email,
                    display_name,
                    authorization_kind: SenderAuthorizationKind::SendOnBehalf.as_str().to_string(),
                    sender_address: Some(principal.email),
                    sender_display: Some(principal.display_name),
                });
            }
        }

        Ok(identities)
    }
}
