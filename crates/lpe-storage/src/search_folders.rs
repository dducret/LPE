use anyhow::{anyhow, bail, Result};
use serde::Serialize;
use serde_json::Value;
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{CanonicalChangeCategory, SearchFolderRow, Storage};

#[derive(Debug, Clone, Serialize)]
pub struct SearchFolderDefinition {
    pub id: Uuid,
    pub account_id: Uuid,
    pub role: String,
    pub display_name: String,
    pub definition_kind: String,
    pub result_object_kind: String,
    pub scope_json: Value,
    pub restriction_json: Value,
    pub excluded_folder_roles: Vec<String>,
    pub is_builtin: bool,
}

#[derive(Debug, Clone)]
pub struct UpsertSearchFolderInput {
    pub id: Option<Uuid>,
    pub account_id: Uuid,
    pub display_name: String,
    pub result_object_kind: String,
    pub scope_json: Value,
    pub restriction_json: Value,
    pub excluded_folder_roles: Vec<String>,
}

struct BuiltinSearchFolderDefinition {
    role: &'static str,
    display_name: &'static str,
    result_object_kind: &'static str,
    scope_json: Value,
    restriction_json: Value,
    excluded_folder_roles: Vec<String>,
}

fn exchange_builtin_search_folder_definitions() -> Vec<BuiltinSearchFolderDefinition> {
    let top_ipm_scope = serde_json::json!({
        "scope": "top_of_personal_folders",
        "recursive": true
    });
    let excluded_mail_roles = vec![
        "trash",
        "junk",
        "drafts",
        "outbox",
        "conflicts",
        "local_failures",
        "server_failures",
        "sync_issues",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();

    vec![
        BuiltinSearchFolderDefinition {
            role: "reminders",
            display_name: "Reminders",
            result_object_kind: "mixed",
            scope_json: top_ipm_scope.clone(),
            restriction_json: serde_json::json!({
                "kind": "exchange_reminders",
                "match": "reminder_set_or_recurring",
                "recurrenceHorizonDays": 90,
                "occurrenceDismissals": true
            }),
            excluded_folder_roles: excluded_mail_roles.clone(),
        },
        BuiltinSearchFolderDefinition {
            role: "todo_search",
            display_name: "To-Do",
            result_object_kind: "mixed",
            scope_json: top_ipm_scope.clone(),
            restriction_json: serde_json::json!({
                "kind": "exchange_todo"
            }),
            excluded_folder_roles: excluded_mail_roles.clone(),
        },
        BuiltinSearchFolderDefinition {
            role: "contacts_search",
            display_name: "Contacts Search",
            result_object_kind: "contact",
            scope_json: serde_json::json!({
                "scope": "contacts_folders",
                "recursive": false
            }),
            restriction_json: serde_json::json!({
                "kind": "exchange_contacts_search"
            }),
            excluded_folder_roles: Vec::new(),
        },
        BuiltinSearchFolderDefinition {
            role: "tracked_mail_processing",
            display_name: "Tracked Mail Processing",
            result_object_kind: "message",
            scope_json: top_ipm_scope,
            restriction_json: serde_json::json!({
                "kind": "exchange_tracked_mail_processing"
            }),
            excluded_folder_roles: excluded_mail_roles,
        },
    ]
}

fn map_search_folder(row: SearchFolderRow) -> SearchFolderDefinition {
    SearchFolderDefinition {
        id: row.id,
        account_id: row.account_id,
        role: row.role,
        display_name: row.display_name,
        definition_kind: row.definition_kind,
        result_object_kind: row.result_object_kind,
        scope_json: row.scope_json,
        restriction_json: row.restriction_json,
        excluded_folder_roles: row.excluded_folder_roles,
        is_builtin: row.is_builtin,
    }
}

fn validate_search_folder_input(input: &UpsertSearchFolderInput) -> Result<()> {
    if input.display_name.trim().is_empty() {
        bail!("search folder display name is required");
    }
    if !matches!(
        input.result_object_kind.as_str(),
        "message" | "contact" | "task" | "mixed"
    ) {
        bail!("unsupported search folder result object kind");
    }
    if !input.scope_json.is_object() {
        bail!("search folder scope must be a JSON object");
    }
    if !input.restriction_json.is_object() {
        bail!("search folder restriction must be a JSON object");
    }
    Ok(())
}

impl Storage {
    pub async fn fetch_search_folders(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<SearchFolderDefinition>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, SearchFolderRow>(
            r#"
            SELECT
                id,
                account_id,
                role,
                display_name,
                definition_kind,
                result_object_kind,
                scope_json,
                restriction_json,
                excluded_folder_roles,
                is_builtin
            FROM search_folders
            WHERE tenant_id = $1
              AND account_id = $2
            ORDER BY is_builtin DESC, display_name ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_search_folder).collect())
    }

    pub async fn fetch_search_folders_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<SearchFolderDefinition>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, SearchFolderRow>(
            r#"
            SELECT
                id,
                account_id,
                role,
                display_name,
                definition_kind,
                result_object_kind,
                scope_json,
                restriction_json,
                excluded_folder_roles,
                is_builtin
            FROM search_folders
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = ANY($3)
            ORDER BY is_builtin DESC, display_name ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_search_folder).collect())
    }

    pub async fn upsert_search_folder(
        &self,
        input: UpsertSearchFolderInput,
    ) -> Result<SearchFolderDefinition> {
        validate_search_folder_input(&input)?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let mut tx = self.pool.begin().await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;
        let display_name = input.display_name.trim();
        let (row, existed) = if let Some(id) = input.id {
            let existed = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM search_folders
                    WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                )
                "#,
            )
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
            let row = sqlx::query_as::<_, SearchFolderRow>(
                r#"
                INSERT INTO search_folders (
                    id, tenant_id, account_id, role, display_name, definition_kind,
                    result_object_kind, scope_json, restriction_json, excluded_folder_roles,
                    is_builtin
                )
                VALUES ($1, $2, $3, 'custom', $4, 'user_saved', $5, $6, $7, $8, FALSE)
                ON CONFLICT (id)
                DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    result_object_kind = EXCLUDED.result_object_kind,
                    scope_json = EXCLUDED.scope_json,
                    restriction_json = EXCLUDED.restriction_json,
                    excluded_folder_roles = EXCLUDED.excluded_folder_roles,
                    updated_at = NOW()
                WHERE search_folders.tenant_id = EXCLUDED.tenant_id
                  AND search_folders.account_id = EXCLUDED.account_id
                  AND NOT search_folders.is_builtin
                RETURNING
                    id,
                    account_id,
                    role,
                    display_name,
                    definition_kind,
                    result_object_kind,
                    scope_json,
                    restriction_json,
                    excluded_folder_roles,
                    is_builtin
                "#,
            )
            .bind(id)
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(display_name)
            .bind(&input.result_object_kind)
            .bind(&input.scope_json)
            .bind(&input.restriction_json)
            .bind(&input.excluded_folder_roles)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| {
                anyhow!("search folder not found or cannot update builtin search folder")
            })?;
            (row, existed)
        } else {
            let existed = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM search_folders
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND NOT is_builtin
                      AND definition_kind = 'user_saved'
                      AND lower(btrim(display_name)) = lower(btrim($3))
                      AND result_object_kind = $4
                )
                "#,
            )
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(display_name)
            .bind(&input.result_object_kind)
            .fetch_one(&mut *tx)
            .await?;
            let row = sqlx::query_as::<_, SearchFolderRow>(
                r#"
                INSERT INTO search_folders (
                    id, tenant_id, account_id, role, display_name, definition_kind,
                    result_object_kind, scope_json, restriction_json, excluded_folder_roles,
                    is_builtin
                )
                VALUES ($1, $2, $3, 'custom', $4, 'user_saved', $5, $6, $7, $8, FALSE)
                ON CONFLICT (
                    tenant_id,
                    account_id,
                    (lower(btrim(display_name))),
                    result_object_kind
                )
                WHERE NOT is_builtin AND definition_kind = 'user_saved'
                DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    scope_json = EXCLUDED.scope_json,
                    restriction_json = EXCLUDED.restriction_json,
                    excluded_folder_roles = EXCLUDED.excluded_folder_roles,
                    updated_at = NOW()
                RETURNING
                    id,
                    account_id,
                    role,
                    display_name,
                    definition_kind,
                    result_object_kind,
                    scope_json,
                    restriction_json,
                    excluded_folder_roles,
                    is_builtin
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(display_name)
            .bind(&input.result_object_kind)
            .bind(&input.scope_json)
            .bind(&input.restriction_json)
            .bind(&input.excluded_folder_roles)
            .fetch_one(&mut *tx)
            .await?;
            (row, existed)
        };

        let change_kind = if existed { "updated" } else { "created" };
        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                CanonicalChangeCategory::Search.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(input.account_id),
            None,
            "search_folder_definition",
            row.id,
            change_kind,
            modseq,
            &[input.account_id],
            serde_json::json!({
                "definitionKind": row.definition_kind,
                "resultObjectKind": row.result_object_kind
            }),
        )
        .await?;
        Self::emit_account_scoped_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Search,
            input.account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(map_search_folder(row))
    }

    pub async fn delete_search_folder(
        &self,
        account_id: Uuid,
        search_folder_id: Uuid,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let is_builtin = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT is_builtin
            FROM search_folders
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(search_folder_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("search folder not found"))?;
        if is_builtin {
            bail!("builtin search folders cannot be deleted");
        }
        self.insert_collaboration_tombstone_in_tx(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Search,
            account_id,
            None,
            "search_folder_definition",
            search_folder_id,
            None,
            &[account_id],
        )
        .await?;
        sqlx::query(
            r#"
            DELETE FROM search_folders
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(search_folder_id)
        .execute(&mut *tx)
        .await?;
        Self::emit_account_scoped_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Search,
            account_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn ensure_exchange_search_folders(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
    ) -> Result<()> {
        for definition in exchange_builtin_search_folder_definitions() {
            let changed = sqlx::query(
                r#"
                INSERT INTO search_folders (
                    id, tenant_id, account_id, role, display_name, definition_kind,
                    result_object_kind, scope_json, restriction_json, excluded_folder_roles,
                    is_builtin
                )
                VALUES ($1, $2, $3, $4, $5, 'exchange_builtin', $6, $7, $8, $9, TRUE)
                ON CONFLICT (tenant_id, account_id, role) WHERE is_builtin
                DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    definition_kind = EXCLUDED.definition_kind,
                    result_object_kind = EXCLUDED.result_object_kind,
                    scope_json = EXCLUDED.scope_json,
                    restriction_json = EXCLUDED.restriction_json,
                    excluded_folder_roles = EXCLUDED.excluded_folder_roles,
                    updated_at = NOW()
                WHERE search_folders.display_name IS DISTINCT FROM EXCLUDED.display_name
                   OR search_folders.definition_kind IS DISTINCT FROM EXCLUDED.definition_kind
                   OR search_folders.result_object_kind IS DISTINCT FROM EXCLUDED.result_object_kind
                   OR search_folders.scope_json IS DISTINCT FROM EXCLUDED.scope_json
                   OR search_folders.restriction_json IS DISTINCT FROM EXCLUDED.restriction_json
                   OR search_folders.excluded_folder_roles IS DISTINCT FROM EXCLUDED.excluded_folder_roles
                RETURNING id, (xmax = 0) AS inserted
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(account_id)
            .bind(definition.role)
            .bind(definition.display_name)
            .bind(definition.result_object_kind)
            .bind(definition.scope_json)
            .bind(definition.restriction_json)
            .bind(definition.excluded_folder_roles)
            .fetch_optional(&mut **tx)
            .await?;

            if let Some(row) = changed {
                let search_folder_id: Uuid = row.try_get("id")?;
                let change_kind = if row.try_get::<bool, _>("inserted")? {
                    "created"
                } else {
                    "updated"
                };
                let modseq = self
                    .allocate_account_modseq_in_tx(
                        tx,
                        tenant_id,
                        account_id,
                        CanonicalChangeCategory::Search.as_str(),
                    )
                    .await?;
                Self::insert_mail_change_log_in_tx(
                    tx,
                    tenant_id,
                    Some(account_id),
                    None,
                    "search_folder_definition",
                    search_folder_id,
                    change_kind,
                    modseq,
                    &[account_id],
                    serde_json::json!({
                        "role": definition.role,
                        "definitionKind": "exchange_builtin",
                        "resultObjectKind": definition.result_object_kind
                    }),
                )
                .await?;
                Self::emit_account_scoped_change(
                    tx,
                    tenant_id,
                    CanonicalChangeCategory::Search,
                    account_id,
                )
                .await?;
            }
        }
        Ok(())
    }
}
