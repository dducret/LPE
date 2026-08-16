use anyhow::{bail, Result};
use lpe_core::sieve::parse_script;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    validate_sieve_script_content, validate_sieve_script_name, AuditEntryInput,
    CanonicalChangeCategory, Storage, MAX_SIEVE_SCRIPTS_PER_ACCOUNT,
};

impl Storage {
    pub async fn replace_active_sieve_script(
        &self,
        account_id: Uuid,
        name: &str,
        expected_content: Option<&str>,
        replacement: Option<&str>,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let name = validate_sieve_script_name(name)?;
        let replacement = replacement.map(validate_sieve_script_content).transpose()?;
        if let Some(content) = &replacement {
            parse_script(content)?;
        }

        let mut tx = self.pool.begin().await?;
        let account_exists = sqlx::query(
            r#"
            SELECT 1
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            FOR UPDATE
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?;
        if account_exists.is_none() {
            bail!("account not found");
        }

        let active = sqlx::query(
            r#"
            SELECT id, name, content
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2 AND is_active = TRUE
            LIMIT 1
            FOR UPDATE
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?;

        let active_matches = match (&active, expected_content) {
            (Some(row), Some(expected)) => {
                let active_name: String = row.try_get("name")?;
                let active_content: String = row.try_get("content")?;
                active_name.eq_ignore_ascii_case(&name) && active_content == expected
            }
            (None, None) => true,
            _ => false,
        };
        if !active_matches {
            bail!(
                "active sieve script changed concurrently or is not the expected generated script"
            );
        }

        let (script_id, change_kind) = match (active, replacement) {
            (Some(active), Some(content)) => {
                let script_id: Uuid = active.try_get("id")?;
                sqlx::query(
                    r#"
                    UPDATE sieve_scripts
                    SET content = $4, updated_at = NOW()
                    WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                    "#,
                )
                .bind(&tenant_id)
                .bind(account_id)
                .bind(script_id)
                .bind(&content)
                .execute(&mut *tx)
                .await?;
                (script_id, "updated")
            }
            (None, Some(content)) => {
                let existing_count = sqlx::query_scalar::<_, i64>(
                    r#"
                    SELECT COUNT(*)
                    FROM sieve_scripts
                    WHERE tenant_id = $1 AND account_id = $2
                    "#,
                )
                .bind(&tenant_id)
                .bind(account_id)
                .fetch_one(&mut *tx)
                .await?;
                if existing_count >= MAX_SIEVE_SCRIPTS_PER_ACCOUNT {
                    bail!("too many sieve scripts for account");
                }
                let existing = sqlx::query_scalar::<_, bool>(
                    r#"
                    SELECT EXISTS(
                        SELECT 1
                        FROM sieve_scripts
                        WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
                    )
                    "#,
                )
                .bind(&tenant_id)
                .bind(account_id)
                .bind(&name)
                .fetch_one(&mut *tx)
                .await?;
                if existing {
                    bail!("generated sieve script name is already in use");
                }
                let script_id = Uuid::new_v4();
                sqlx::query(
                    r#"
                    INSERT INTO sieve_scripts (id, tenant_id, account_id, name, content, is_active)
                    VALUES ($1, $2, $3, $4, $5, TRUE)
                    "#,
                )
                .bind(script_id)
                .bind(&tenant_id)
                .bind(account_id)
                .bind(&name)
                .bind(&content)
                .execute(&mut *tx)
                .await?;
                (script_id, "created")
            }
            (Some(active), None) => {
                let script_id: Uuid = active.try_get("id")?;
                self.insert_collaboration_tombstone_in_tx(
                    &mut tx,
                    &tenant_id,
                    CanonicalChangeCategory::Rules,
                    account_id,
                    None,
                    "sieve_script",
                    script_id,
                    Some(&name),
                    &[account_id],
                )
                .await?;
                sqlx::query(
                    r#"
                    DELETE FROM sieve_scripts
                    WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                    "#,
                )
                .bind(&tenant_id)
                .bind(account_id)
                .bind(script_id)
                .execute(&mut *tx)
                .await?;
                (script_id, "destroyed")
            }
            (None, None) => bail!("generated sieve script is not active"),
        };

        if change_kind != "destroyed" {
            let modseq = self
                .allocate_account_modseq_in_tx(
                    &mut tx,
                    &tenant_id,
                    account_id,
                    CanonicalChangeCategory::Rules.as_str(),
                )
                .await?;
            Self::insert_mail_change_log_in_tx(
                &mut tx,
                &tenant_id,
                Some(account_id),
                None,
                "sieve_script",
                script_id,
                change_kind,
                modseq,
                &[account_id],
                serde_json::json!({
                    "name": name,
                    "activeScriptChanged": true,
                }),
            )
            .await?;
        }
        Self::emit_account_scoped_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Rules,
            account_id,
        )
        .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }
}
