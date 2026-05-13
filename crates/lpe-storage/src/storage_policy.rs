use anyhow::{anyhow, bail, Result};
use sqlx::Row;
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    storage_backend::{
        normalize_storage_pool_config, normalize_storage_pool_kind, select_storage_backend,
        storage_pool_config_summary,
    },
    AuditEntryInput, NewStoragePool, Storage, StoragePolicyOverview, StoragePolicyScope,
    StoragePolicySummary, StoragePolicyUpdate, StoragePoolReference, StoragePoolSummary,
    UpdateStoragePool, PLATFORM_TENANT_ID,
};

#[derive(Debug, Clone)]
struct PoolRow {
    id: Uuid,
    name: String,
    pool_kind: String,
    status: String,
    config_json: serde_json::Value,
    is_platform_default: bool,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Clone)]
struct AssignmentRow {
    scope_kind: String,
    tenant_id: Option<Uuid>,
    domain_id: Option<Uuid>,
    account_id: Option<Uuid>,
    storage_pool_id: Uuid,
    updated_by: String,
    updated_at: String,
}

#[derive(Debug, Clone)]
struct TenantTarget {
    tenant_id: Uuid,
    tenant_name: String,
}

#[derive(Debug, Clone)]
struct DomainTarget {
    tenant_id: Uuid,
    tenant_name: String,
    domain_id: Uuid,
    domain_name: String,
}

#[derive(Debug, Clone)]
struct AccountTarget {
    tenant_id: Uuid,
    tenant_name: String,
    domain_id: Uuid,
    account_id: Uuid,
    account_email: String,
}

impl Storage {
    pub async fn list_storage_pools(
        &self,
        include_disabled: bool,
    ) -> Result<Vec<StoragePoolSummary>> {
        let pools = self.load_storage_pool_rows(include_disabled).await?;
        Ok(pools.into_iter().map(storage_pool_summary).collect())
    }

    pub async fn create_storage_pool(
        &self,
        input: NewStoragePool,
        audit: AuditEntryInput,
    ) -> Result<StoragePoolSummary> {
        let name = normalize_storage_pool_name(&input.name)?;
        let pool_kind = normalize_storage_pool_kind(&input.pool_kind)?;
        let config_json = normalize_storage_pool_config(pool_kind, input.config)?;
        let status = normalize_storage_pool_status(&input.status)?;
        let pool_id = Uuid::new_v4();
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            INSERT INTO storage_pools (id, name, pool_kind, status, config_json)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING
                id,
                name,
                pool_kind,
                status,
                config_json,
                FALSE AS is_platform_default,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(pool_id)
        .bind(name)
        .bind(pool_kind)
        .bind(status)
        .bind(config_json)
        .fetch_one(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, PLATFORM_TENANT_ID, audit)
            .await?;
        tx.commit().await?;
        storage_pool_summary_from_row(row)
    }

    pub async fn update_storage_pool(
        &self,
        input: UpdateStoragePool,
        audit: AuditEntryInput,
    ) -> Result<StoragePoolSummary> {
        let name = normalize_storage_pool_name(&input.name)?;
        let status = normalize_storage_pool_status(&input.status)?;
        let existing = self.load_storage_pool_row(input.pool_id).await?;
        let config_json = match input.config {
            Some(config) => normalize_storage_pool_config(&existing.pool_kind, Some(config))?,
            None => existing.config_json,
        };
        if status == "disabled" {
            self.ensure_storage_pool_can_be_disabled(input.pool_id)
                .await?;
        }

        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            UPDATE storage_pools
            SET name = $2,
                status = $3,
                config_json = $4,
                updated_at = NOW()
            WHERE id = $1
            RETURNING
                id,
                name,
                pool_kind,
                status,
                config_json,
                id = (
                    SELECT storage_pool_id
                    FROM storage_policy_assignments
                    WHERE scope_kind = 'platform'
                    LIMIT 1
                ) AS is_platform_default,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(input.pool_id)
        .bind(name)
        .bind(status)
        .bind(config_json)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("storage pool not found"))?;

        self.insert_audit(&mut tx, PLATFORM_TENANT_ID, audit)
            .await?;
        tx.commit().await?;
        storage_pool_summary_from_row(row)
    }

    pub async fn fetch_platform_storage_policy_overview(&self) -> Result<StoragePolicyOverview> {
        self.fetch_storage_policy_overview(None).await
    }

    pub async fn fetch_tenant_storage_policy_overview(
        &self,
        tenant_id: Uuid,
    ) -> Result<StoragePolicyOverview> {
        self.ensure_tenant_exists(tenant_id).await?;
        self.fetch_storage_policy_overview(Some(tenant_id)).await
    }

    pub async fn set_platform_storage_policy(
        &self,
        update: StoragePolicyUpdate,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let pool_id = update
            .storage_pool_id
            .ok_or_else(|| anyhow!("platform storage policy requires a storage pool"))?;
        self.ensure_active_storage_pool(pool_id).await?;
        self.replace_storage_policy_assignment("platform", None, None, None, Some(pool_id), audit)
            .await
    }

    pub async fn set_tenant_storage_policy(
        &self,
        tenant_id: Uuid,
        update: StoragePolicyUpdate,
        audit: AuditEntryInput,
    ) -> Result<()> {
        self.ensure_tenant_exists(tenant_id).await?;
        if let Some(pool_id) = update.storage_pool_id {
            self.ensure_active_storage_pool(pool_id).await?;
        }
        self.replace_storage_policy_assignment(
            "tenant",
            Some(tenant_id),
            None,
            None,
            update.storage_pool_id,
            audit,
        )
        .await
    }

    pub async fn set_domain_storage_policy(
        &self,
        domain_id: Uuid,
        update: StoragePolicyUpdate,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.storage_policy_tenant_for_domain(domain_id).await?;
        if let Some(pool_id) = update.storage_pool_id {
            self.ensure_active_storage_pool(pool_id).await?;
        }
        self.replace_storage_policy_assignment(
            "domain",
            Some(tenant_id),
            Some(domain_id),
            None,
            update.storage_pool_id,
            audit,
        )
        .await
    }

    pub async fn set_account_storage_policy(
        &self,
        account_id: Uuid,
        update: StoragePolicyUpdate,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let (tenant_id, _) = self
            .storage_policy_tenant_and_domain_for_account(account_id)
            .await?;
        if let Some(pool_id) = update.storage_pool_id {
            self.ensure_active_storage_pool(pool_id).await?;
        }
        self.replace_storage_policy_assignment(
            "account",
            Some(tenant_id),
            None,
            Some(account_id),
            update.storage_pool_id,
            audit,
        )
        .await
    }

    pub async fn storage_policy_tenant_for_domain(&self, domain_id: Uuid) -> Result<Uuid> {
        sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT tenant_id
            FROM domains
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(domain_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("domain not found"))
    }

    pub async fn storage_policy_tenant_and_domain_for_account(
        &self,
        account_id: Uuid,
    ) -> Result<(Uuid, Uuid)> {
        sqlx::query_as::<_, (Uuid, Uuid)>(
            r#"
            SELECT tenant_id, primary_domain_id
            FROM accounts
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("account not found"))
    }

    async fn fetch_storage_policy_overview(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<StoragePolicyOverview> {
        let pools = self.load_storage_pool_rows(true).await?;
        let pool_map = pools
            .iter()
            .map(|pool| (pool.id, storage_pool_reference(pool)))
            .collect::<HashMap<_, _>>();
        let allowed_pools = pools
            .iter()
            .filter(|pool| pool.status == "active")
            .cloned()
            .map(storage_pool_summary)
            .collect::<Vec<_>>();
        let assignments = self.load_storage_policy_assignments(tenant_filter).await?;
        let assignment_map = assignments
            .iter()
            .map(|assignment| (assignment_key(assignment), assignment.clone()))
            .collect::<HashMap<_, _>>();

        let platform_assignment = assignments
            .iter()
            .find(|assignment| assignment.scope_kind == "platform");
        let platform_pool = platform_assignment
            .and_then(|assignment| pool_map.get(&assignment.storage_pool_id).cloned())
            .or_else(|| {
                pools
                    .iter()
                    .find(|pool| pool.status == "active")
                    .map(storage_pool_reference)
            })
            .ok_or_else(|| anyhow!("no active storage pool is available"))?;

        let mut policies = Vec::new();
        if tenant_filter.is_none() {
            policies.push(policy_summary(
                StoragePolicyScope {
                    kind: "platform".to_string(),
                    tenant_id: None,
                    tenant_name: None,
                    domain_id: None,
                    domain_name: None,
                    account_id: None,
                    account_email: None,
                    name: "Platform default".to_string(),
                },
                platform_assignment,
                &platform_pool,
                None,
                &pool_map,
            )?);
        }

        let tenants = self.load_storage_policy_tenants(tenant_filter).await?;
        let domains = self.load_storage_policy_domains(tenant_filter).await?;
        let accounts = self.load_storage_policy_accounts(tenant_filter).await?;

        for tenant in tenants {
            let tenant_assignment = assignment_map.get(&format!("tenant:{}", tenant.tenant_id));
            let tenant_pool = assignment_pool(tenant_assignment, &pool_map)
                .unwrap_or_else(|| platform_pool.clone());
            let inherited_from = tenant_assignment.is_none().then(|| "platform".to_string());
            policies.push(policy_summary(
                StoragePolicyScope {
                    kind: "tenant".to_string(),
                    tenant_id: Some(tenant.tenant_id),
                    tenant_name: Some(tenant.tenant_name.clone()),
                    domain_id: None,
                    domain_name: None,
                    account_id: None,
                    account_email: None,
                    name: tenant.tenant_name,
                },
                tenant_assignment,
                &tenant_pool,
                inherited_from,
                &pool_map,
            )?);
        }

        for domain in domains {
            let tenant_assignment = assignment_map.get(&format!("tenant:{}", domain.tenant_id));
            let tenant_pool = assignment_pool(tenant_assignment, &pool_map)
                .unwrap_or_else(|| platform_pool.clone());
            let domain_assignment = assignment_map.get(&format!("domain:{}", domain.domain_id));
            let domain_pool = assignment_pool(domain_assignment, &pool_map)
                .unwrap_or_else(|| tenant_pool.clone());
            let inherited_from = if domain_assignment.is_some() {
                None
            } else if tenant_assignment.is_some() {
                Some("tenant".to_string())
            } else {
                Some("platform".to_string())
            };
            policies.push(policy_summary(
                StoragePolicyScope {
                    kind: "domain".to_string(),
                    tenant_id: Some(domain.tenant_id),
                    tenant_name: Some(domain.tenant_name),
                    domain_id: Some(domain.domain_id),
                    domain_name: Some(domain.domain_name.clone()),
                    account_id: None,
                    account_email: None,
                    name: domain.domain_name,
                },
                domain_assignment,
                &domain_pool,
                inherited_from,
                &pool_map,
            )?);
        }

        for account in accounts {
            let tenant_assignment = assignment_map.get(&format!("tenant:{}", account.tenant_id));
            let tenant_pool = assignment_pool(tenant_assignment, &pool_map)
                .unwrap_or_else(|| platform_pool.clone());
            let domain_assignment = assignment_map.get(&format!("domain:{}", account.domain_id));
            let domain_pool = assignment_pool(domain_assignment, &pool_map)
                .unwrap_or_else(|| tenant_pool.clone());
            let account_assignment = assignment_map.get(&format!("account:{}", account.account_id));
            let account_pool = assignment_pool(account_assignment, &pool_map)
                .unwrap_or_else(|| domain_pool.clone());
            let inherited_from = if account_assignment.is_some() {
                None
            } else if domain_assignment.is_some() {
                Some("domain".to_string())
            } else if tenant_assignment.is_some() {
                Some("tenant".to_string())
            } else {
                Some("platform".to_string())
            };
            policies.push(policy_summary(
                StoragePolicyScope {
                    kind: "account".to_string(),
                    tenant_id: Some(account.tenant_id),
                    tenant_name: Some(account.tenant_name),
                    domain_id: Some(account.domain_id),
                    domain_name: None,
                    account_id: Some(account.account_id),
                    account_email: Some(account.account_email.clone()),
                    name: account.account_email,
                },
                account_assignment,
                &account_pool,
                inherited_from,
                &pool_map,
            )?);
        }

        Ok(StoragePolicyOverview {
            allowed_pools,
            policies,
        })
    }

    async fn load_storage_pool_rows(&self, include_disabled: bool) -> Result<Vec<PoolRow>> {
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                name,
                pool_kind,
                status,
                config_json,
                id = (
                    SELECT storage_pool_id
                    FROM storage_policy_assignments
                    WHERE scope_kind = 'platform'
                    LIMIT 1
                ) AS is_platform_default,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM storage_pools
            WHERE $1 OR status = 'active'
            ORDER BY created_at ASC, name ASC
            "#,
        )
        .bind(include_disabled)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(pool_row_from_row).collect()
    }

    async fn load_storage_pool_row(&self, pool_id: Uuid) -> Result<PoolRow> {
        let row = sqlx::query(
            r#"
            SELECT
                id,
                name,
                pool_kind,
                status,
                config_json,
                id = (
                    SELECT storage_pool_id
                    FROM storage_policy_assignments
                    WHERE scope_kind = 'platform'
                    LIMIT 1
                ) AS is_platform_default,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM storage_pools
            WHERE id = $1
            "#,
        )
        .bind(pool_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("storage pool not found"))?;
        pool_row_from_row(row)
    }

    async fn load_storage_policy_assignments(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<Vec<AssignmentRow>> {
        let rows = sqlx::query(
            r#"
            SELECT
                scope_kind,
                tenant_id,
                domain_id,
                account_id,
                storage_pool_id,
                updated_by,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM storage_policy_assignments
            WHERE scope_kind = 'platform'
               OR $1::uuid IS NULL
               OR tenant_id = $1
            ORDER BY scope_kind ASC, updated_at DESC
            "#,
        )
        .bind(tenant_filter)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(assignment_row_from_row).collect()
    }

    async fn load_storage_policy_tenants(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<Vec<TenantTarget>> {
        let rows = sqlx::query(
            r#"
            SELECT id, display_name
            FROM tenants
            WHERE $1::uuid IS NULL OR id = $1
            ORDER BY display_name ASC
            "#,
        )
        .bind(tenant_filter)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(TenantTarget {
                    tenant_id: row.try_get("id")?,
                    tenant_name: row.try_get("display_name")?,
                })
            })
            .collect()
    }

    async fn load_storage_policy_domains(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<Vec<DomainTarget>> {
        let rows = sqlx::query(
            r#"
            SELECT
                d.tenant_id,
                t.display_name AS tenant_name,
                d.id AS domain_id,
                d.name AS domain_name
            FROM domains d
            JOIN tenants t ON t.id = d.tenant_id
            WHERE $1::uuid IS NULL OR d.tenant_id = $1
            ORDER BY d.name ASC
            "#,
        )
        .bind(tenant_filter)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(DomainTarget {
                    tenant_id: row.try_get("tenant_id")?,
                    tenant_name: row.try_get("tenant_name")?,
                    domain_id: row.try_get("domain_id")?,
                    domain_name: row.try_get("domain_name")?,
                })
            })
            .collect()
    }

    async fn load_storage_policy_accounts(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<Vec<AccountTarget>> {
        let rows = sqlx::query(
            r#"
            SELECT
                a.tenant_id,
                t.display_name AS tenant_name,
                a.primary_domain_id AS domain_id,
                a.id AS account_id,
                a.primary_email AS account_email
            FROM accounts a
            JOIN tenants t ON t.id = a.tenant_id
            WHERE $1::uuid IS NULL OR a.tenant_id = $1
            ORDER BY a.primary_email ASC
            "#,
        )
        .bind(tenant_filter)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(AccountTarget {
                    tenant_id: row.try_get("tenant_id")?,
                    tenant_name: row.try_get("tenant_name")?,
                    domain_id: row.try_get("domain_id")?,
                    account_id: row.try_get("account_id")?,
                    account_email: row.try_get("account_email")?,
                })
            })
            .collect()
    }

    async fn replace_storage_policy_assignment(
        &self,
        scope_kind: &str,
        tenant_id: Option<Uuid>,
        domain_id: Option<Uuid>,
        account_id: Option<Uuid>,
        storage_pool_id: Option<Uuid>,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            DELETE FROM storage_policy_assignments
            WHERE scope_kind = $1
              AND (
                  ($1 = 'platform' AND tenant_id IS NULL AND domain_id IS NULL AND account_id IS NULL)
                  OR ($1 = 'tenant' AND tenant_id = $2)
                  OR ($1 = 'domain' AND domain_id = $3)
                  OR ($1 = 'account' AND account_id = $4)
              )
            "#,
        )
        .bind(scope_kind)
        .bind(tenant_id)
        .bind(domain_id)
        .bind(account_id)
        .execute(&mut *tx)
        .await?;

        if let Some(pool_id) = storage_pool_id {
            sqlx::query(
                r#"
                INSERT INTO storage_policy_assignments (
                    id, scope_kind, tenant_id, domain_id, account_id, storage_pool_id, updated_by
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(scope_kind)
            .bind(tenant_id)
            .bind(domain_id)
            .bind(account_id)
            .bind(pool_id)
            .bind(&audit.actor)
            .execute(&mut *tx)
            .await?;
        }

        let audit_tenant = tenant_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| PLATFORM_TENANT_ID.to_string());
        self.insert_audit(&mut tx, &audit_tenant, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn ensure_tenant_exists(&self, tenant_id: Uuid) -> Result<()> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM tenants
                WHERE id = $1
            )
            "#,
        )
        .bind(tenant_id)
        .fetch_one(&self.pool)
        .await?;
        if exists {
            Ok(())
        } else {
            bail!("tenant not found")
        }
    }

    async fn ensure_active_storage_pool(&self, pool_id: Uuid) -> Result<()> {
        let pool = self.load_storage_pool_row(pool_id).await?;
        if pool.status != "active" {
            bail!("storage policy must reference an active storage pool");
        }
        select_storage_backend(&pool.pool_kind, &pool.config_json)?;
        Ok(())
    }

    async fn ensure_storage_pool_can_be_disabled(&self, pool_id: Uuid) -> Result<()> {
        let active_pools = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM storage_pools WHERE status = 'active'",
        )
        .fetch_one(&self.pool)
        .await?;
        if active_pools <= 1 {
            bail!("cannot disable the only active storage pool");
        }

        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM storage_policy_assignments
                WHERE storage_pool_id = $1
            )
            "#,
        )
        .bind(pool_id)
        .fetch_one(&self.pool)
        .await?
        {
            bail!("cannot disable a storage pool referenced by storage policy");
        }

        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM blob_placements
                WHERE storage_pool_id = $1
                  AND placement_status IN ('active', 'copying', 'verified', 'retiring')
            )
            "#,
        )
        .bind(pool_id)
        .fetch_one(&self.pool)
        .await?
        {
            bail!("cannot disable a storage pool with live blob placements");
        }

        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM blob_migration_jobs
                WHERE (source_storage_pool_id = $1 OR target_storage_pool_id = $1)
                  AND status IN ('pending', 'running', 'verified')
            )
            "#,
        )
        .bind(pool_id)
        .fetch_one(&self.pool)
        .await?
        {
            bail!("cannot disable a storage pool with open migration jobs");
        }

        Ok(())
    }
}

fn normalize_storage_pool_name(name: &str) -> Result<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed != trimmed.to_ascii_lowercase() {
        bail!("storage pool name must be lowercase and non-empty");
    }
    Ok(trimmed.to_string())
}

fn normalize_storage_pool_status(status: &str) -> Result<&'static str> {
    match status.trim() {
        "active" => Ok("active"),
        "disabled" => Ok("disabled"),
        _ => bail!("unsupported storage pool status"),
    }
}

fn pool_row_from_row(row: sqlx::postgres::PgRow) -> Result<PoolRow> {
    Ok(PoolRow {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        pool_kind: row.try_get("pool_kind")?,
        status: row.try_get("status")?,
        config_json: row.try_get("config_json")?,
        is_platform_default: row.try_get("is_platform_default")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn storage_pool_summary_from_row(row: sqlx::postgres::PgRow) -> Result<StoragePoolSummary> {
    pool_row_from_row(row).map(storage_pool_summary)
}

fn storage_pool_summary(pool: PoolRow) -> StoragePoolSummary {
    let config = storage_pool_config_summary(&pool.pool_kind, &pool.config_json)
        .ok()
        .flatten();
    let assignable = pool.status == "active"
        && select_storage_backend(&pool.pool_kind, &pool.config_json).is_ok();
    StoragePoolSummary {
        id: pool.id,
        name: pool.name,
        pool_kind: pool.pool_kind,
        status: pool.status,
        assignable,
        config,
        is_platform_default: pool.is_platform_default,
        created_at: pool.created_at,
        updated_at: pool.updated_at,
    }
}

fn storage_pool_reference(pool: &PoolRow) -> StoragePoolReference {
    StoragePoolReference {
        id: pool.id,
        name: pool.name.clone(),
        pool_kind: pool.pool_kind.clone(),
        status: pool.status.clone(),
    }
}

fn assignment_row_from_row(row: sqlx::postgres::PgRow) -> Result<AssignmentRow> {
    Ok(AssignmentRow {
        scope_kind: row.try_get("scope_kind")?,
        tenant_id: row.try_get("tenant_id")?,
        domain_id: row.try_get("domain_id")?,
        account_id: row.try_get("account_id")?,
        storage_pool_id: row.try_get("storage_pool_id")?,
        updated_by: row.try_get("updated_by")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn assignment_key(assignment: &AssignmentRow) -> String {
    match assignment.scope_kind.as_str() {
        "platform" => "platform".to_string(),
        "tenant" => format!(
            "tenant:{}",
            assignment.tenant_id.expect("tenant policy tenant_id")
        ),
        "domain" => format!(
            "domain:{}",
            assignment.domain_id.expect("domain policy domain_id")
        ),
        "account" => format!(
            "account:{}",
            assignment.account_id.expect("account policy account_id")
        ),
        _ => "unsupported".to_string(),
    }
}

fn assignment_pool(
    assignment: Option<&AssignmentRow>,
    pool_map: &HashMap<Uuid, StoragePoolReference>,
) -> Option<StoragePoolReference> {
    assignment.and_then(|assignment| pool_map.get(&assignment.storage_pool_id).cloned())
}

fn policy_summary(
    scope: StoragePolicyScope,
    assignment: Option<&AssignmentRow>,
    effective_pool: &StoragePoolReference,
    inherited_from: Option<String>,
    pool_map: &HashMap<Uuid, StoragePoolReference>,
) -> Result<StoragePolicySummary> {
    let configured_pool = assignment_pool(assignment, pool_map);
    if assignment.is_some() && configured_pool.is_none() {
        bail!("storage policy references an unknown storage pool");
    }
    Ok(StoragePolicySummary {
        scope,
        configured_pool,
        effective_pool: effective_pool.clone(),
        inherited_from,
        updated_at: assignment.map(|assignment| assignment.updated_at.clone()),
        updated_by: assignment.map(|assignment| assignment.updated_by.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Storage;
    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;

    const SCHEMA: &str = include_str!("../sql/schema.sql");

    async fn test_storage() -> Option<Storage> {
        let database_url = match std::env::var("LPE_STORAGE_TEST_DATABASE_URL") {
            Ok(value) => value,
            Err(_) => return None,
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("connect to LPE_STORAGE_TEST_DATABASE_URL");
        sqlx::raw_sql("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            .execute(&pool)
            .await
            .expect("reset test database schema");
        sqlx::raw_sql(SCHEMA)
            .execute(&pool)
            .await
            .expect("apply schema.sql to test database");
        Some(Storage::new(pool))
    }

    fn audit(action: &str) -> AuditEntryInput {
        AuditEntryInput {
            actor: "admin@example.test".to_string(),
            action: action.to_string(),
            subject: "storage policy".to_string(),
        }
    }

    async fn insert_tenant_domain_account(storage: &Storage) -> (Uuid, Uuid, Uuid) {
        let tenant_id = Uuid::new_v4();
        let domain_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO tenants (id, slug, display_name) VALUES ($1, 'policy-test', 'Policy Test')")
            .bind(tenant_id)
            .execute(storage.pool())
            .await
            .expect("insert tenant");
        sqlx::query("INSERT INTO domains (id, tenant_id, name) VALUES ($1, $2, 'policy.test')")
            .bind(domain_id)
            .bind(tenant_id)
            .execute(storage.pool())
            .await
            .expect("insert domain");
        sqlx::query(
            "INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name) VALUES ($1, $2, $3, 'alice@policy.test', 'Alice')",
        )
        .bind(account_id)
        .bind(tenant_id)
        .bind(domain_id)
        .execute(storage.pool())
        .await
        .expect("insert account");
        (tenant_id, domain_id, account_id)
    }

    async fn create_secondary_pool(storage: &Storage) -> Uuid {
        storage
            .create_storage_pool(
                NewStoragePool {
                    name: "postgres-secondary".to_string(),
                    pool_kind: "postgres".to_string(),
                    status: "active".to_string(),
                    config: None,
                },
                audit("create-storage-pool"),
            )
            .await
            .expect("create secondary pool")
            .id
    }

    #[tokio::test]
    async fn tenant_domain_and_account_policy_inherit_and_clear() {
        let Some(storage) = test_storage().await else {
            return;
        };
        let (tenant_id, domain_id, account_id) = insert_tenant_domain_account(&storage).await;
        let secondary_pool_id = create_secondary_pool(&storage).await;

        storage
            .set_tenant_storage_policy(
                tenant_id,
                StoragePolicyUpdate {
                    storage_pool_id: Some(secondary_pool_id),
                },
                audit("set-tenant-storage-policy"),
            )
            .await
            .expect("set tenant policy");

        let overview = storage
            .fetch_tenant_storage_policy_overview(tenant_id)
            .await
            .expect("fetch tenant overview");
        let account_policy = overview
            .policies
            .iter()
            .find(|policy| policy.scope.account_id == Some(account_id))
            .expect("account policy");
        assert_eq!(account_policy.effective_pool.id, secondary_pool_id);
        assert_eq!(account_policy.inherited_from.as_deref(), Some("tenant"));

        storage
            .set_domain_storage_policy(
                domain_id,
                StoragePolicyUpdate {
                    storage_pool_id: None,
                },
                audit("clear-domain-storage-policy"),
            )
            .await
            .expect("clear empty domain policy");
        storage
            .set_account_storage_policy(
                account_id,
                StoragePolicyUpdate {
                    storage_pool_id: None,
                },
                audit("clear-account-storage-policy"),
            )
            .await
            .expect("clear empty account policy");
    }

    #[tokio::test]
    async fn policy_rejects_disabled_or_unknown_pool() {
        let Some(storage) = test_storage().await else {
            return;
        };
        let (tenant_id, _, _) = insert_tenant_domain_account(&storage).await;
        let secondary_pool_id = create_secondary_pool(&storage).await;
        storage
            .update_storage_pool(
                UpdateStoragePool {
                    pool_id: secondary_pool_id,
                    name: "postgres-secondary".to_string(),
                    status: "disabled".to_string(),
                    config: None,
                },
                audit("disable-storage-pool"),
            )
            .await
            .expect("disable unused pool");

        let disabled_error = storage
            .set_tenant_storage_policy(
                tenant_id,
                StoragePolicyUpdate {
                    storage_pool_id: Some(secondary_pool_id),
                },
                audit("set-tenant-storage-policy"),
            )
            .await
            .expect_err("disabled pool should be rejected")
            .to_string();
        assert!(disabled_error.contains("active storage pool"));

        let unknown_error = storage
            .set_tenant_storage_policy(
                tenant_id,
                StoragePolicyUpdate {
                    storage_pool_id: Some(Uuid::new_v4()),
                },
                audit("set-tenant-storage-policy"),
            )
            .await
            .expect_err("unknown pool should be rejected")
            .to_string();
        assert!(unknown_error.contains("storage pool not found"));
    }

    #[tokio::test]
    async fn s3_compatible_pool_config_is_redacted_in_summary() {
        let Some(storage) = test_storage().await else {
            return;
        };

        let summary = storage
            .create_storage_pool(
                NewStoragePool {
                    name: "object-main".to_string(),
                    pool_kind: "s3_compatible".to_string(),
                    status: "active".to_string(),
                    config: Some(json!({
                        "endpointUrl": "https://objects.example.test/",
                        "bucket": "lpe-blobs",
                        "signingRegion": "local",
                        "addressingStyle": "path-style",
                        "objectPrefix": "/mail/blobs/",
                        "credentialsRef": "env:LPE_STORAGE_POOL_MAIN"
                    })),
                },
                audit("create-storage-pool"),
            )
            .await
            .expect("create s3-compatible pool");

        assert_eq!(summary.pool_kind, "s3_compatible");
        assert!(summary.assignable);
        let config = summary.config.expect("redacted config");
        assert_eq!(
            config.endpoint_url.as_deref(),
            Some("https://objects.example.test")
        );
        assert_eq!(config.object_prefix.as_deref(), Some("mail/blobs"));
        assert!(config.credentials_configured);
        let serialized = serde_json::to_string(&config).expect("serialize summary");
        assert!(!serialized.contains("LPE_STORAGE_POOL_MAIN"));
        assert!(!serialized.contains("credentialsRef"));
    }

    #[tokio::test]
    async fn policy_changes_do_not_create_migration_jobs() {
        let Some(storage) = test_storage().await else {
            return;
        };
        let (tenant_id, _, _) = insert_tenant_domain_account(&storage).await;
        let secondary_pool_id = create_secondary_pool(&storage).await;
        storage
            .set_tenant_storage_policy(
                tenant_id,
                StoragePolicyUpdate {
                    storage_pool_id: Some(secondary_pool_id),
                },
                audit("set-tenant-storage-policy"),
            )
            .await
            .expect("set tenant policy");

        let jobs = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM blob_migration_jobs")
            .fetch_one(storage.pool())
            .await
            .expect("count migration jobs");
        assert_eq!(jobs, 0);
    }

    #[tokio::test]
    async fn policy_change_records_admin_audit_event() {
        let Some(storage) = test_storage().await else {
            return;
        };
        let (tenant_id, _, _) = insert_tenant_domain_account(&storage).await;
        let secondary_pool_id = create_secondary_pool(&storage).await;

        storage
            .set_tenant_storage_policy(
                tenant_id,
                StoragePolicyUpdate {
                    storage_pool_id: Some(secondary_pool_id),
                },
                AuditEntryInput {
                    actor: "ops@example.test".to_string(),
                    action: "update-tenant-storage-policy".to_string(),
                    subject: format!(
                        "scope=tenant:{tenant_id} storage_pool_id={secondary_pool_id}"
                    ),
                },
            )
            .await
            .expect("set tenant policy");

        let row = sqlx::query(
            r#"
            SELECT actor, action, subject
            FROM audit_events
            WHERE tenant_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(tenant_id.to_string())
        .fetch_one(storage.pool())
        .await
        .expect("audit row");

        assert_eq!(
            row.try_get::<String, _>("actor").unwrap(),
            "ops@example.test"
        );
        assert_eq!(
            row.try_get::<String, _>("action").unwrap(),
            "update-tenant-storage-policy"
        );
        assert_eq!(
            row.try_get::<String, _>("subject").unwrap(),
            format!("scope=tenant:{tenant_id} storage_pool_id={secondary_pool_id}")
        );
    }
}
