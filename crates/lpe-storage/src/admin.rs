use anyhow::{anyhow, bail, Result};
use lpe_core::sieve::parse_script;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    env_bind_address, env_hostname, normalize_admin_permissions, normalize_email,
    permission_summary, permissions_from_storage, validate_sieve_script_content,
    validate_sieve_script_name, AntispamSettings, AuditEntryInput, CanonicalChangeCategory,
    DashboardUpdate, EmailTraceResult, EmailTraceRow, EmailTraceSearchInput, FilterRule,
    LocalAiSettings, MailFlowEntry, MailFlowRow, MailboxRule, NewServerAdministrator,
    OutlookProfileState, SecuritySettings, ServerAdministrator, ServerAdministratorRow,
    ServerSettings, SieveScriptDocument, SieveScriptSummary, Storage,
    MAX_SIEVE_SCRIPTS_PER_ACCOUNT, PLATFORM_TENANT_ID,
};

mod dashboard;
mod helpers;
mod provisioning;

use helpers::{
    count_from_row, mailbox_rule_summaries, map_email_trace_row, map_mail_flow_row,
    unsupported_client_local_profile_state, unsupported_exchange_rule_features,
};

impl Storage {
    pub async fn record_platform_audit(&self, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.insert_audit(&mut tx, &PLATFORM_TENANT_ID, audit)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn create_server_administrator(
        &self,
        input: NewServerAdministrator,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = match input.domain_id {
            Some(domain_id) => self.tenant_id_for_domain_id(domain_id).await?,
            None => PLATFORM_TENANT_ID,
        };
        let normalized_permissions =
            normalize_admin_permissions(&input.role, &input.rights_summary, &input.permissions);
        sqlx::query(
            r#"
            INSERT INTO server_administrators (
                id, tenant_id, domain_id, email, display_name, role, rights_summary, permissions_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(input.domain_id)
        .bind(input.email.trim().to_lowercase())
        .bind(input.display_name.trim())
        .bind(input.role.trim())
        .bind(permission_summary(&normalized_permissions))
        .bind(serde_json::to_string(&normalized_permissions)?)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn find_server_administrator_by_email(
        &self,
        email: &str,
    ) -> Result<Option<ServerAdministrator>> {
        let email = normalize_email(email);
        let tenant_id = self.tenant_id_for_admin_email(&email).await?;
        let row = sqlx::query_as::<_, ServerAdministratorRow>(
            r#"
            SELECT
                sa.id,
                sa.domain_id,
                d.name AS domain_name,
                sa.email,
                sa.display_name,
                sa.role,
                sa.rights_summary,
                sa.permissions_json
            FROM server_administrators sa
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE sa.tenant_id = $1
              AND lower(sa.email) = lower($2)
            ORDER BY sa.created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| {
            let permissions = permissions_from_storage(
                &row.role,
                Some(&row.rights_summary),
                Some(&row.permissions_json),
            );
            ServerAdministrator {
                id: row.id,
                domain_id: row.domain_id,
                domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
                email: row.email,
                display_name: row.display_name,
                role: row.role,
                rights_summary: permission_summary(&permissions),
                permissions,
            }
        }))
    }

    pub async fn append_audit_event(&self, tenant_id: Uuid, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn list_sieve_scripts(&self, account_id: Uuid) -> Result<Vec<SieveScriptSummary>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                name,
                is_active,
                octet_length(content)::BIGINT AS size_octets,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY lower(name) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(SieveScriptSummary {
                    name: row.try_get("name")?,
                    is_active: row.try_get("is_active")?,
                    size_octets: row.try_get::<i64, _>("size_octets")?.max(0) as u64,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect()
    }

    pub async fn list_mailbox_rules(&self, account_id: Uuid) -> Result<Vec<MailboxRule>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                name,
                content,
                is_active,
                octet_length(content)::BIGINT AS size_octets,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY is_active DESC, lower(name) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let content: String = row.try_get("content")?;
                let (condition_summary, action_summary) = mailbox_rule_summaries(&content);
                Ok(MailboxRule {
                    id: row.try_get("id")?,
                    name: row.try_get("name")?,
                    is_active: row.try_get("is_active")?,
                    source_kind: "sieve_script".to_string(),
                    condition_summary,
                    action_summary,
                    supported_outlook_projection: true,
                    unsupported_exchange_features: unsupported_exchange_rule_features(),
                    size_octets: row.try_get::<i64, _>("size_octets")?.max(0) as u64,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect()
    }

    pub async fn fetch_outlook_profile_state(
        &self,
        account_id: Uuid,
    ) -> Result<OutlookProfileState> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query(
            r#"
            SELECT
                (SELECT COUNT(*)
                 FROM search_folders
                 WHERE tenant_id = $1 AND account_id = $2) AS search_folders_count,
                (SELECT COUNT(*)
                 FROM sieve_scripts
                 WHERE tenant_id = $1 AND account_id = $2) AS rules_count,
                (SELECT COUNT(*)
                 FROM account_identities
                 WHERE tenant_id = $1 AND account_id = $2) AS sender_identities_count,
                (SELECT COUNT(*)
                 FROM mapi_named_properties
                 WHERE tenant_id = $1 AND account_id = $2) AS mapi_named_properties_count,
                (SELECT COUNT(*)
                 FROM mapi_custom_property_values
                 WHERE tenant_id = $1 AND account_id = $2) AS mapi_custom_properties_count,
                (SELECT COUNT(*)
                 FROM mapi_navigation_shortcuts
                 WHERE tenant_id = $1 AND account_id = $2) AS mapi_navigation_shortcuts_count,
                (SELECT COUNT(*)
                 FROM mapi_sync_checkpoints
                 WHERE tenant_id = $1 AND account_id = $2) AS mapi_sync_checkpoints_count,
                ps.ipm_subtree_ost_id IS NOT NULL AS ipm_subtree_ost_id_present,
                COALESCE(octet_length(ps.ipm_subtree_ost_id), 0)::BIGINT
                    AS ipm_subtree_ost_id_size_octets,
                to_char(ps.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    AS profile_settings_updated_at
            FROM (SELECT 1) marker
            LEFT JOIN mapi_profile_settings ps
              ON ps.tenant_id = $1
             AND ps.account_id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_one(&self.pool)
        .await?;

        let ipm_subtree_ost_id_present: bool = row.try_get("ipm_subtree_ost_id_present")?;
        Ok(OutlookProfileState {
            id: "profile".to_string(),
            account_id,
            messages_backed_by_canonical_mailbox: true,
            contacts_backed_by_canonical_store: true,
            calendars_backed_by_canonical_store: true,
            tasks_backed_by_canonical_store: true,
            notes_backed_by_canonical_store: true,
            journals_backed_by_canonical_store: true,
            search_folders_count: count_from_row(&row, "search_folders_count")?,
            rules_count: count_from_row(&row, "rules_count")?,
            sender_identities_count: count_from_row(&row, "sender_identities_count")?,
            mapi_named_properties_count: count_from_row(&row, "mapi_named_properties_count")?,
            mapi_custom_properties_count: count_from_row(&row, "mapi_custom_properties_count")?,
            mapi_navigation_shortcuts_count: count_from_row(
                &row,
                "mapi_navigation_shortcuts_count",
            )?,
            mapi_sync_checkpoints_count: count_from_row(&row, "mapi_sync_checkpoints_count")?,
            mapi_profile_settings_present: ipm_subtree_ost_id_present,
            ipm_subtree_ost_id_present,
            ipm_subtree_ost_id_size_octets: count_from_row(&row, "ipm_subtree_ost_id_size_octets")?,
            profile_settings_updated_at: row.try_get("profile_settings_updated_at")?,
            unsupported_client_local_state: unsupported_client_local_profile_state(),
        })
    }

    pub async fn fetch_mapi_ipm_subtree_ost_id(&self, account_id: Uuid) -> Result<Option<Vec<u8>>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        Ok(sqlx::query_scalar::<_, Vec<u8>>(
            r#"
            SELECT ipm_subtree_ost_id
            FROM mapi_profile_settings
            WHERE tenant_id = $1 AND account_id = $2
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn store_mapi_ipm_subtree_ost_id(
        &self,
        account_id: Uuid,
        ost_id: &[u8],
    ) -> Result<()> {
        if ost_id.is_empty() || ost_id.len() > 2048 {
            bail!("invalid MAPI IPM subtree OST identity");
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        sqlx::query(
            r#"
            INSERT INTO mapi_profile_settings (
                tenant_id,
                account_id,
                ipm_subtree_ost_id
            )
            VALUES ($1, $2, $3)
            ON CONFLICT (tenant_id, account_id)
            DO UPDATE SET
                ipm_subtree_ost_id = EXCLUDED.ipm_subtree_ost_id,
                updated_at = NOW()
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ost_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_sieve_script(
        &self,
        account_id: Uuid,
        name: &str,
    ) -> Result<Option<SieveScriptDocument>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let normalized_name = validate_sieve_script_name(name)?;
        let row = sqlx::query(
            r#"
            SELECT
                name,
                content,
                is_active,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&normalized_name)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(SieveScriptDocument {
                name: row.try_get("name")?,
                content: row.try_get("content")?,
                is_active: row.try_get("is_active")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .transpose()
    }

    pub async fn put_sieve_script(
        &self,
        account_id: Uuid,
        name: &str,
        content: &str,
        activate: bool,
        audit: AuditEntryInput,
    ) -> Result<SieveScriptDocument> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let name = validate_sieve_script_name(name)?;
        let content = validate_sieve_script_content(content)?;
        parse_script(&content)?;

        let mut tx = self.pool.begin().await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;

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

        let exists = sqlx::query_scalar::<_, bool>(
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

        if !exists && existing_count >= MAX_SIEVE_SCRIPTS_PER_ACCOUNT {
            bail!("too many sieve scripts for account");
        }

        if activate {
            sqlx::query(
                r#"
                UPDATE sieve_scripts
                SET is_active = FALSE, updated_at = NOW()
                WHERE tenant_id = $1 AND account_id = $2 AND is_active = TRUE
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .execute(&mut *tx)
            .await?;
        }

        let row = sqlx::query(
            r#"
            INSERT INTO sieve_scripts (id, tenant_id, account_id, name, content, is_active)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, account_id, normalized_name) DO UPDATE SET
                name = EXCLUDED.name,
                content = EXCLUDED.content,
                is_active = EXCLUDED.is_active OR sieve_scripts.is_active,
                updated_at = NOW()
            RETURNING
                id,
                name,
                content,
                is_active,
                (xmax = 0) AS inserted,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&name)
        .bind(&content)
        .bind(activate)
        .fetch_one(&mut *tx)
        .await?;

        let script_id: Uuid = row.try_get("id")?;
        let change_kind = if row.try_get::<bool, _>("inserted")? {
            "created"
        } else {
            "updated"
        };
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
                "isActive": row.try_get::<bool, _>("is_active")?,
            }),
        )
        .await?;
        Self::emit_account_scoped_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Rules,
            account_id,
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        Ok(SieveScriptDocument {
            name: row.try_get("name")?,
            content: row.try_get("content")?,
            is_active: row.try_get("is_active")?,
            updated_at: row.try_get("updated_at")?,
        })
    }

    pub async fn delete_sieve_script(
        &self,
        account_id: Uuid,
        name: &str,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let name = validate_sieve_script_name(name)?;
        let mut tx = self.pool.begin().await?;

        let script = sqlx::query(
            r#"
            SELECT id, is_active
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("sieve script not found"))?;
        let script_id: Uuid = script.try_get("id")?;
        let active: bool = script.try_get("is_active")?;

        if active {
            bail!("cannot delete the active sieve script");
        }

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
            WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&name)
        .execute(&mut *tx)
        .await?;

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

    pub async fn rename_sieve_script(
        &self,
        account_id: Uuid,
        old_name: &str,
        new_name: &str,
        audit: AuditEntryInput,
    ) -> Result<SieveScriptSummary> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let old_name = validate_sieve_script_name(old_name)?;
        let new_name = validate_sieve_script_name(new_name)?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            UPDATE sieve_scripts
            SET name = $4, updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            RETURNING
                id,
                name,
                is_active,
                octet_length(content)::BIGINT AS size_octets,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&old_name)
        .bind(&new_name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("sieve script not found"))?;

        let script_id: Uuid = row.try_get("id")?;
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
            "updated",
            modseq,
            &[account_id],
            serde_json::json!({
                "name": new_name,
                "previousName": old_name,
                "isActive": row.try_get::<bool, _>("is_active")?,
            }),
        )
        .await?;
        Self::emit_account_scoped_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Rules,
            account_id,
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        Ok(SieveScriptSummary {
            name: row.try_get("name")?,
            is_active: row.try_get("is_active")?,
            size_octets: row.try_get::<i64, _>("size_octets")?.max(0) as u64,
            updated_at: row.try_get("updated_at")?,
        })
    }

    pub async fn set_active_sieve_script(
        &self,
        account_id: Uuid,
        name: Option<&str>,
        audit: AuditEntryInput,
    ) -> Result<Option<String>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deactivated = sqlx::query(
            r#"
            UPDATE sieve_scripts
            SET is_active = FALSE, updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND is_active = TRUE
            RETURNING id, name
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&mut *tx)
        .await?;

        let mut changed_scripts = deactivated
            .into_iter()
            .map(|row| {
                Ok::<_, anyhow::Error>((
                    row.try_get::<Uuid, _>("id")?,
                    row.try_get::<String, _>("name")?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let active_name = if let Some(name) = name {
            let name = validate_sieve_script_name(name)?;
            let updated = sqlx::query(
                r#"
                UPDATE sieve_scripts
                SET is_active = TRUE, updated_at = NOW()
                WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
                RETURNING id, name
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(&name)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow!("sieve script not found"))?;
            let active_id: Uuid = updated.try_get("id")?;
            let active_name: String = updated.try_get("name")?;
            changed_scripts.push((active_id, active_name.clone()));
            Some(active_name)
        } else {
            None
        };

        changed_scripts.sort_by_key(|(_, script_name)| script_name.clone());
        changed_scripts.dedup_by_key(|(script_id, _)| *script_id);
        let has_rule_changes = !changed_scripts.is_empty();
        for (script_id, script_name) in changed_scripts {
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
                "updated",
                modseq,
                &[account_id],
                serde_json::json!({
                    "name": script_name,
                    "activeScriptChanged": true,
                }),
            )
            .await?;
        }
        if has_rule_changes {
            Self::emit_account_scoped_change(
                &mut tx,
                &tenant_id,
                CanonicalChangeCategory::Rules,
                account_id,
            )
            .await?;
        }
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(active_name)
    }

    pub async fn fetch_active_sieve_script(
        &self,
        account_id: Uuid,
    ) -> Result<Option<SieveScriptDocument>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query(
            r#"
            SELECT
                name,
                content,
                is_active,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2 AND is_active = TRUE
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(SieveScriptDocument {
                name: row.try_get("name")?,
                content: row.try_get("content")?,
                is_active: row.try_get("is_active")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .transpose()
    }

    pub async fn create_filter_rule(
        &self,
        input: crate::NewFilterRule,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let _ = (input, audit);
        bail!("perimeter filtering rules are managed by LPE-CT")
    }

    pub async fn update_settings(
        &self,
        update: DashboardUpdate,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO server_settings (
                tenant_id, primary_hostname, admin_bind_address, smtp_bind_address,
                imap_bind_address, jmap_bind_address, default_locale, max_message_size_mb, tls_mode
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id) DO UPDATE SET
                primary_hostname = EXCLUDED.primary_hostname,
                admin_bind_address = EXCLUDED.admin_bind_address,
                smtp_bind_address = EXCLUDED.smtp_bind_address,
                imap_bind_address = EXCLUDED.imap_bind_address,
                jmap_bind_address = EXCLUDED.jmap_bind_address,
                default_locale = EXCLUDED.default_locale,
                max_message_size_mb = EXCLUDED.max_message_size_mb,
                tls_mode = EXCLUDED.tls_mode,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.server_settings.primary_hostname)
        .bind(update.server_settings.admin_bind_address)
        .bind(update.server_settings.smtp_bind_address)
        .bind(update.server_settings.imap_bind_address)
        .bind(update.server_settings.jmap_bind_address)
        .bind(update.server_settings.default_locale)
        .bind(update.server_settings.max_message_size_mb as i32)
        .bind(update.server_settings.tls_mode)
        .execute(&mut *tx)
        .await?;
        // keep rest of update logic in lib for now? no, full body continues
        sqlx::query(
            r#"
            INSERT INTO security_settings (
                tenant_id, password_login_enabled, mfa_required_for_admins,
                session_timeout_minutes, audit_retention_days, oidc_login_enabled,
                oidc_provider_label, oidc_auto_link_by_email,
                mailbox_password_login_enabled, mailbox_oidc_login_enabled,
                mailbox_oidc_provider_label, mailbox_oidc_auto_link_by_email,
                mailbox_app_passwords_enabled
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (tenant_id) DO UPDATE SET
                password_login_enabled = EXCLUDED.password_login_enabled,
                mfa_required_for_admins = EXCLUDED.mfa_required_for_admins,
                session_timeout_minutes = EXCLUDED.session_timeout_minutes,
                audit_retention_days = EXCLUDED.audit_retention_days,
                oidc_login_enabled = EXCLUDED.oidc_login_enabled,
                oidc_provider_label = EXCLUDED.oidc_provider_label,
                oidc_auto_link_by_email = EXCLUDED.oidc_auto_link_by_email,
                mailbox_password_login_enabled = EXCLUDED.mailbox_password_login_enabled,
                mailbox_oidc_login_enabled = EXCLUDED.mailbox_oidc_login_enabled,
                mailbox_oidc_provider_label = EXCLUDED.mailbox_oidc_provider_label,
                mailbox_oidc_auto_link_by_email = EXCLUDED.mailbox_oidc_auto_link_by_email,
                mailbox_app_passwords_enabled = EXCLUDED.mailbox_app_passwords_enabled,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.security_settings.password_login_enabled)
        .bind(update.security_settings.mfa_required_for_admins)
        .bind(update.security_settings.session_timeout_minutes as i32)
        .bind(update.security_settings.audit_retention_days as i32)
        .bind(update.security_settings.oidc_login_enabled)
        .bind(update.security_settings.oidc_provider_label)
        .bind(update.security_settings.oidc_auto_link_by_email)
        .bind(update.security_settings.mailbox_password_login_enabled)
        .bind(update.security_settings.mailbox_oidc_login_enabled)
        .bind(update.security_settings.mailbox_oidc_provider_label)
        .bind(update.security_settings.mailbox_oidc_auto_link_by_email)
        .bind(update.security_settings.mailbox_app_passwords_enabled)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO admin_oidc_config (
                tenant_id, issuer_url, authorization_endpoint, token_endpoint, userinfo_endpoint,
                client_id, client_secret, scopes, claim_email, claim_display_name, claim_subject
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (tenant_id) DO UPDATE SET
                issuer_url = EXCLUDED.issuer_url,
                authorization_endpoint = EXCLUDED.authorization_endpoint,
                token_endpoint = EXCLUDED.token_endpoint,
                userinfo_endpoint = EXCLUDED.userinfo_endpoint,
                client_id = EXCLUDED.client_id,
                client_secret = EXCLUDED.client_secret,
                scopes = EXCLUDED.scopes,
                claim_email = EXCLUDED.claim_email,
                claim_display_name = EXCLUDED.claim_display_name,
                claim_subject = EXCLUDED.claim_subject,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.security_settings.oidc_issuer_url)
        .bind(update.security_settings.oidc_authorization_endpoint)
        .bind(update.security_settings.oidc_token_endpoint)
        .bind(update.security_settings.oidc_userinfo_endpoint)
        .bind(update.security_settings.oidc_client_id)
        .bind(update.security_settings.oidc_client_secret)
        .bind(update.security_settings.oidc_scopes)
        .bind(update.security_settings.oidc_claim_email)
        .bind(update.security_settings.oidc_claim_display_name)
        .bind(update.security_settings.oidc_claim_subject)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO account_oidc_config (
                tenant_id, issuer_url, authorization_endpoint, token_endpoint, userinfo_endpoint,
                client_id, client_secret, scopes, claim_email, claim_display_name, claim_subject
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (tenant_id) DO UPDATE SET
                issuer_url = EXCLUDED.issuer_url,
                authorization_endpoint = EXCLUDED.authorization_endpoint,
                token_endpoint = EXCLUDED.token_endpoint,
                userinfo_endpoint = EXCLUDED.userinfo_endpoint,
                client_id = EXCLUDED.client_id,
                client_secret = EXCLUDED.client_secret,
                scopes = EXCLUDED.scopes,
                claim_email = EXCLUDED.claim_email,
                claim_display_name = EXCLUDED.claim_display_name,
                claim_subject = EXCLUDED.claim_subject,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.security_settings.mailbox_oidc_issuer_url)
        .bind(update.security_settings.mailbox_oidc_authorization_endpoint)
        .bind(update.security_settings.mailbox_oidc_token_endpoint)
        .bind(update.security_settings.mailbox_oidc_userinfo_endpoint)
        .bind(update.security_settings.mailbox_oidc_client_id)
        .bind(update.security_settings.mailbox_oidc_client_secret)
        .bind(update.security_settings.mailbox_oidc_scopes)
        .bind(update.security_settings.mailbox_oidc_claim_email)
        .bind(update.security_settings.mailbox_oidc_claim_display_name)
        .bind(update.security_settings.mailbox_oidc_claim_subject)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO local_ai_settings (
                tenant_id, enabled, provider, model, offline_only, indexing_enabled
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                provider = EXCLUDED.provider,
                model = EXCLUDED.model,
                offline_only = EXCLUDED.offline_only,
                indexing_enabled = EXCLUDED.indexing_enabled,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.local_ai_settings.enabled)
        .bind(update.local_ai_settings.provider)
        .bind(update.local_ai_settings.model)
        .bind(update.local_ai_settings.offline_only)
        .bind(update.local_ai_settings.indexing_enabled)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &PLATFORM_TENANT_ID, audit)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_mail_flow_entries(&self) -> Result<Vec<MailFlowEntry>> {
        let rows = sqlx::query_as::<_, MailFlowRow>(
            r#"
            SELECT
                q.id AS queue_id,
                m.id AS message_id,
                a.primary_email AS account_email,
                m.normalized_subject AS subject,
                m.internet_message_id,
                q.status,
                q.status AS delivery_status,
                TRUE AS was_submitted,
                (mb.role = 'sent' AND m.sent_at IS NOT NULL) AS in_sent_mailbox,
                q.attempts,
                to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS submitted_at,
                CASE
                    WHEN m.sent_at IS NULL THEN NULL
                    ELSE to_char(m.sent_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS sent_at,
                CASE
                    WHEN q.last_attempt_at IS NULL THEN NULL
                    ELSE to_char(q.last_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS last_attempt_at,
                CASE
                    WHEN q.next_attempt_at IS NULL THEN NULL
                    ELSE to_char(q.next_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS next_attempt_at,
                q.last_trace_id AS trace_id,
                q.remote_message_ref,
                q.last_error,
                NULL::integer AS retry_after_seconds,
                NULL::text AS retry_policy,
                NULL::text AS last_dsn_status,
                NULL::integer AS last_smtp_code,
                NULL::text AS last_enhanced_status
            FROM submission_queue q
            JOIN mailbox_messages smm
              ON smm.tenant_id = q.tenant_id
             AND smm.account_id = q.account_id
             AND smm.id = q.sent_mailbox_message_id
            JOIN messages m
              ON m.tenant_id = q.tenant_id
             AND m.id = smm.message_id
            JOIN mailboxes mb
              ON mb.tenant_id = smm.tenant_id
             AND mb.account_id = smm.account_id
             AND mb.id = smm.mailbox_id
            JOIN accounts a
              ON a.tenant_id = q.tenant_id
             AND a.id = q.account_id
            WHERE q.tenant_id = $1
            ORDER BY q.created_at DESC
            LIMIT 100
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_mail_flow_row).collect())
    }

    pub async fn search_email_trace(
        &self,
        input: EmailTraceSearchInput,
    ) -> Result<Vec<EmailTraceResult>> {
        let like_query = format!("%{}%", input.query.trim().to_lowercase());
        let rows = sqlx::query_as::<_, EmailTraceRow>(
            r#"
            SELECT
                m.id AS message_id,
                m.internet_message_id,
                m.normalized_subject AS subject,
                COALESCE(fr.address, '') AS sender,
                a.primary_email AS account_email,
                mb.display_name AS mailbox,
                COALESCE(q.queue_status, CASE WHEN mm.is_draft THEN 'draft' ELSE 'stored' END) AS delivery_status,
                (q.queue_status IS NOT NULL) AS was_submitted,
                (mb.role = 'sent' AND m.sent_at IS NOT NULL) AS in_sent_mailbox,
                CASE
                    WHEN m.sent_at IS NULL THEN NULL
                    ELSE to_char(m.sent_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS sent_at,
                q.queue_status,
                COALESCE(q.latest_trace_id, inbound.latest_trace_id) AS latest_trace_id,
                q.remote_message_ref,
                q.last_attempt_at,
                q.next_attempt_at,
                q.last_error,
                q.last_dsn_status,
                q.last_smtp_code,
                q.last_enhanced_status,
                to_char(m.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
             AND mm.visibility <> 'expunged'
            JOIN mailboxes mb
              ON mb.tenant_id = mm.tenant_id
             AND mb.account_id = mm.account_id
             AND mb.id = mm.mailbox_id
            JOIN accounts a
              ON a.tenant_id = mm.tenant_id
             AND a.id = mm.account_id
            LEFT JOIN message_recipients fr
              ON fr.tenant_id = m.tenant_id
             AND fr.message_id = m.id
             AND fr.role = 'from'
            LEFT JOIN LATERAL (
                SELECT
                    q.status AS queue_status,
                    q.last_trace_id AS latest_trace_id,
                    q.remote_message_ref,
                    CASE
                        WHEN q.last_attempt_at IS NULL THEN NULL
                        ELSE to_char(q.last_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    END AS last_attempt_at,
                    CASE
                        WHEN q.next_attempt_at IS NULL THEN NULL
                        ELSE to_char(q.next_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    END AS next_attempt_at,
                    q.last_error,
                    NULL::text AS last_dsn_status,
                    NULL::integer AS last_smtp_code,
                    NULL::text AS last_enhanced_status
                FROM submission_queue q
                JOIN mailbox_messages qmm
                  ON qmm.tenant_id = q.tenant_id
                 AND qmm.account_id = q.account_id
                 AND qmm.id = q.sent_mailbox_message_id
                WHERE q.tenant_id = m.tenant_id
                  AND qmm.message_id = m.id
                ORDER BY q.created_at DESC
                LIMIT 1
            ) q ON TRUE
            LEFT JOIN LATERAL (
                SELECT h.header_value AS latest_trace_id
                FROM message_headers h
                WHERE h.tenant_id = m.tenant_id
                  AND h.message_id = m.id
                  AND lower(h.header_name) = 'x-lpe-ct-trace-id'
                ORDER BY h.ordinal DESC
                LIMIT 1
            ) inbound ON TRUE
            WHERE m.tenant_id = $1
              AND (
                lower(COALESCE(fr.address, '')) LIKE $2
                OR lower(m.normalized_subject) LIKE $2
                OR lower(COALESCE(m.internet_message_id, '')) LIKE $2
                OR lower(a.primary_email) LIKE $2
                OR lower(COALESCE(q.latest_trace_id, '')) LIKE $2
                OR lower(COALESCE(inbound.latest_trace_id, '')) LIKE $2
                OR lower(COALESCE(q.remote_message_ref, '')) LIKE $2
                OR EXISTS (
                    SELECT 1
                    FROM attachments at
                    JOIN attachment_texts att
                      ON att.tenant_id = at.tenant_id
                     AND att.blob_id = at.blob_id
                    WHERE at.tenant_id = m.tenant_id
                      AND at.message_id = m.id
                      AND lower(att.extracted_text) LIKE $2
                )
              )
            ORDER BY m.received_at DESC
            LIMIT 50
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(like_query)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_email_trace_row).collect())
    }

    async fn fetch_server_settings(&self) -> Result<ServerSettings> {
        let row = sqlx::query(
            r#"
            SELECT primary_hostname, admin_bind_address, smtp_bind_address, imap_bind_address,
                   jmap_bind_address, default_locale, max_message_size_mb, tls_mode
            FROM server_settings
            WHERE tenant_id = $1
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(row) => ServerSettings {
                primary_hostname: row.try_get("primary_hostname")?,
                admin_bind_address: row.try_get("admin_bind_address")?,
                smtp_bind_address: row.try_get("smtp_bind_address")?,
                imap_bind_address: row.try_get("imap_bind_address")?,
                jmap_bind_address: row.try_get("jmap_bind_address")?,
                default_locale: row.try_get("default_locale")?,
                max_message_size_mb: row.try_get::<i32, _>("max_message_size_mb")? as u32,
                tls_mode: row.try_get("tls_mode")?,
            },
            None => ServerSettings {
                primary_hostname: env_hostname("LPE_PUBLIC_HOSTNAME")
                    .or_else(|| env_hostname("LPE_SERVER_NAME"))
                    .unwrap_or_else(|| "localhost".to_string()),
                admin_bind_address: env_bind_address("LPE_BIND_ADDRESS", "127.0.0.1:8080"),
                smtp_bind_address: env_bind_address("LPE_SMTP_BIND_ADDRESS", "0.0.0.0:25"),
                imap_bind_address: env_bind_address("LPE_IMAP_BIND_ADDRESS", "0.0.0.0:143"),
                jmap_bind_address: env_bind_address("LPE_JMAP_BIND_ADDRESS", "0.0.0.0:8081"),
                default_locale: "en".to_string(),
                max_message_size_mb: 64,
                tls_mode: "required".to_string(),
            },
        })
    }

    async fn fetch_security_settings(&self) -> Result<SecuritySettings> {
        let row = sqlx::query(
            r#"
            SELECT
                s.password_login_enabled,
                s.mfa_required_for_admins,
                s.session_timeout_minutes,
                s.audit_retention_days,
                s.oidc_login_enabled,
                s.oidc_provider_label,
                s.oidc_auto_link_by_email,
                s.mailbox_password_login_enabled,
                s.mailbox_oidc_login_enabled,
                s.mailbox_oidc_provider_label,
                s.mailbox_oidc_auto_link_by_email,
                s.mailbox_app_passwords_enabled,
                c.issuer_url,
                c.authorization_endpoint,
                c.token_endpoint,
                c.userinfo_endpoint,
                c.client_id,
                c.client_secret,
                c.scopes,
                c.claim_email,
                c.claim_display_name,
                c.claim_subject,
                mc.issuer_url AS mailbox_issuer_url,
                mc.authorization_endpoint AS mailbox_authorization_endpoint,
                mc.token_endpoint AS mailbox_token_endpoint,
                mc.userinfo_endpoint AS mailbox_userinfo_endpoint,
                mc.client_id AS mailbox_client_id,
                mc.client_secret AS mailbox_client_secret,
                mc.scopes AS mailbox_scopes,
                mc.claim_email AS mailbox_claim_email,
                mc.claim_display_name AS mailbox_claim_display_name,
                mc.claim_subject AS mailbox_claim_subject
            FROM security_settings s
            LEFT JOIN admin_oidc_config c ON c.tenant_id = s.tenant_id
            LEFT JOIN account_oidc_config mc ON mc.tenant_id = s.tenant_id
            WHERE s.tenant_id = $1
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(row) => SecuritySettings {
                password_login_enabled: row.try_get("password_login_enabled")?,
                mfa_required_for_admins: row.try_get("mfa_required_for_admins")?,
                session_timeout_minutes: row.try_get::<i32, _>("session_timeout_minutes")? as u32,
                audit_retention_days: row.try_get::<i32, _>("audit_retention_days")? as u32,
                oidc_login_enabled: row.try_get("oidc_login_enabled")?,
                oidc_provider_label: row.try_get("oidc_provider_label")?,
                oidc_auto_link_by_email: row.try_get("oidc_auto_link_by_email")?,
                oidc_issuer_url: row
                    .try_get::<Option<String>, _>("issuer_url")?
                    .unwrap_or_default(),
                oidc_authorization_endpoint: row
                    .try_get::<Option<String>, _>("authorization_endpoint")?
                    .unwrap_or_default(),
                oidc_token_endpoint: row
                    .try_get::<Option<String>, _>("token_endpoint")?
                    .unwrap_or_default(),
                oidc_userinfo_endpoint: row
                    .try_get::<Option<String>, _>("userinfo_endpoint")?
                    .unwrap_or_default(),
                oidc_client_id: row
                    .try_get::<Option<String>, _>("client_id")?
                    .unwrap_or_default(),
                oidc_client_secret: row
                    .try_get::<Option<String>, _>("client_secret")?
                    .unwrap_or_default(),
                oidc_scopes: row
                    .try_get::<Option<String>, _>("scopes")?
                    .unwrap_or_else(|| "openid profile email".to_string()),
                oidc_claim_email: row
                    .try_get::<Option<String>, _>("claim_email")?
                    .unwrap_or_else(|| "email".to_string()),
                oidc_claim_display_name: row
                    .try_get::<Option<String>, _>("claim_display_name")?
                    .unwrap_or_else(|| "name".to_string()),
                oidc_claim_subject: row
                    .try_get::<Option<String>, _>("claim_subject")?
                    .unwrap_or_else(|| "sub".to_string()),
                mailbox_password_login_enabled: row.try_get("mailbox_password_login_enabled")?,
                mailbox_oidc_login_enabled: row.try_get("mailbox_oidc_login_enabled")?,
                mailbox_oidc_provider_label: row.try_get("mailbox_oidc_provider_label")?,
                mailbox_oidc_auto_link_by_email: row.try_get("mailbox_oidc_auto_link_by_email")?,
                mailbox_oidc_issuer_url: row
                    .try_get::<Option<String>, _>("mailbox_issuer_url")?
                    .unwrap_or_default(),
                mailbox_oidc_authorization_endpoint: row
                    .try_get::<Option<String>, _>("mailbox_authorization_endpoint")?
                    .unwrap_or_default(),
                mailbox_oidc_token_endpoint: row
                    .try_get::<Option<String>, _>("mailbox_token_endpoint")?
                    .unwrap_or_default(),
                mailbox_oidc_userinfo_endpoint: row
                    .try_get::<Option<String>, _>("mailbox_userinfo_endpoint")?
                    .unwrap_or_default(),
                mailbox_oidc_client_id: row
                    .try_get::<Option<String>, _>("mailbox_client_id")?
                    .unwrap_or_default(),
                mailbox_oidc_client_secret: row
                    .try_get::<Option<String>, _>("mailbox_client_secret")?
                    .unwrap_or_default(),
                mailbox_oidc_scopes: row
                    .try_get::<Option<String>, _>("mailbox_scopes")?
                    .unwrap_or_else(|| "openid profile email".to_string()),
                mailbox_oidc_claim_email: row
                    .try_get::<Option<String>, _>("mailbox_claim_email")?
                    .unwrap_or_else(|| "email".to_string()),
                mailbox_oidc_claim_display_name: row
                    .try_get::<Option<String>, _>("mailbox_claim_display_name")?
                    .unwrap_or_else(|| "name".to_string()),
                mailbox_oidc_claim_subject: row
                    .try_get::<Option<String>, _>("mailbox_claim_subject")?
                    .unwrap_or_else(|| "sub".to_string()),
                mailbox_app_passwords_enabled: row.try_get("mailbox_app_passwords_enabled")?,
            },
            None => SecuritySettings {
                password_login_enabled: true,
                mfa_required_for_admins: true,
                session_timeout_minutes: 45,
                audit_retention_days: 365,
                oidc_login_enabled: false,
                oidc_provider_label: "Corporate SSO".to_string(),
                oidc_auto_link_by_email: true,
                oidc_issuer_url: String::new(),
                oidc_authorization_endpoint: String::new(),
                oidc_token_endpoint: String::new(),
                oidc_userinfo_endpoint: String::new(),
                oidc_client_id: String::new(),
                oidc_client_secret: String::new(),
                oidc_scopes: "openid profile email".to_string(),
                oidc_claim_email: "email".to_string(),
                oidc_claim_display_name: "name".to_string(),
                oidc_claim_subject: "sub".to_string(),
                mailbox_password_login_enabled: true,
                mailbox_oidc_login_enabled: false,
                mailbox_oidc_provider_label: "Mailbox SSO".to_string(),
                mailbox_oidc_auto_link_by_email: true,
                mailbox_oidc_issuer_url: String::new(),
                mailbox_oidc_authorization_endpoint: String::new(),
                mailbox_oidc_token_endpoint: String::new(),
                mailbox_oidc_userinfo_endpoint: String::new(),
                mailbox_oidc_client_id: String::new(),
                mailbox_oidc_client_secret: String::new(),
                mailbox_oidc_scopes: "openid profile email".to_string(),
                mailbox_oidc_claim_email: "email".to_string(),
                mailbox_oidc_claim_display_name: "name".to_string(),
                mailbox_oidc_claim_subject: "sub".to_string(),
                mailbox_app_passwords_enabled: true,
            },
        })
    }

    async fn fetch_local_ai_settings(&self) -> Result<LocalAiSettings> {
        let row = sqlx::query(
            r#"
            SELECT enabled, provider, model, offline_only, indexing_enabled
            FROM local_ai_settings
            WHERE tenant_id = $1
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(row) => LocalAiSettings {
                enabled: row.try_get("enabled")?,
                provider: row.try_get("provider")?,
                model: row.try_get("model")?,
                offline_only: row.try_get("offline_only")?,
                indexing_enabled: row.try_get("indexing_enabled")?,
            },
            None => LocalAiSettings {
                enabled: true,
                provider: "not-configured".to_string(),
                model: String::new(),
                offline_only: true,
                indexing_enabled: true,
            },
        })
    }

    async fn fetch_antispam_settings(&self) -> Result<AntispamSettings> {
        Ok(AntispamSettings {
            content_filtering_enabled: false,
            spam_engine: "lpe-ct-managed".to_string(),
            quarantine_enabled: false,
            quarantine_retention_days: 0,
        })
    }

    async fn fetch_server_administrators(&self) -> Result<Vec<ServerAdministrator>> {
        let rows = sqlx::query_as::<_, ServerAdministratorRow>(
            r#"
            SELECT
                sa.id,
                sa.domain_id,
                d.name AS domain_name,
                sa.email,
                sa.display_name,
                sa.role,
                sa.rights_summary,
                sa.permissions_json
            FROM server_administrators sa
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE sa.tenant_id = $1
            ORDER BY sa.created_at ASC
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let permissions = permissions_from_storage(
                    &row.role,
                    Some(&row.rights_summary),
                    Some(&row.permissions_json),
                );
                ServerAdministrator {
                    id: row.id,
                    domain_id: row.domain_id,
                    domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
                    email: row.email,
                    display_name: row.display_name,
                    role: row.role,
                    rights_summary: permission_summary(&permissions),
                    permissions,
                }
            })
            .collect())
    }

    async fn fetch_antispam_rules(&self) -> Result<Vec<FilterRule>> {
        Ok(Vec::new())
    }
}
