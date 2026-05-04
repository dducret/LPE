use anyhow::{anyhow, bail, Result};
use serde::Serialize;
use sqlx::Postgres;
use uuid::Uuid;

use crate::{
    collaboration::{validate_collaboration_rights, CollaborationRights},
    normalize_email, normalize_task_list_name, normalize_task_status, AuditEntryInput,
    ClientTaskListRow, ClientTaskRow, DavTaskRow, Storage, TaskListGrantRow,
    DEFAULT_TASK_LIST_NAME, DEFAULT_TASK_LIST_ROLE,
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskListGrant {
    pub id: Uuid,
    pub task_list_id: Uuid,
    pub task_list_name: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub grantee_account_id: Uuid,
    pub grantee_email: String,
    pub grantee_display_name: String,
    pub rights: CollaborationRights,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct TaskListGrantInput {
    pub owner_account_id: Uuid,
    pub task_list_id: Uuid,
    pub grantee_email: String,
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientTaskList {
    pub id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub is_owned: bool,
    pub rights: CollaborationRights,
    pub name: String,
    pub role: Option<String>,
    pub sort_order: i32,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientTask {
    pub id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub is_owned: bool,
    pub rights: CollaborationRights,
    pub task_list_id: Uuid,
    pub task_list_sort_order: i32,
    pub title: String,
    pub description: String,
    pub status: String,
    pub due_at: Option<String>,
    pub completed_at: Option<String>,
    pub sort_order: i32,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DavTask {
    pub id: Uuid,
    pub collection_id: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub rights: CollaborationRights,
    pub task_list_id: Uuid,
    pub task_list_name: String,
    pub title: String,
    pub description: String,
    pub status: String,
    pub due_at: Option<String>,
    pub completed_at: Option<String>,
    pub sort_order: i32,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct CreateTaskListInput {
    pub account_id: Uuid,
    pub name: String,
    pub sort_order: i32,
}

#[derive(Debug, Clone)]
pub struct UpdateTaskListInput {
    pub account_id: Uuid,
    pub task_list_id: Uuid,
    pub name: Option<String>,
    pub sort_order: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct UpsertClientTaskInput {
    pub id: Option<Uuid>,
    pub principal_account_id: Uuid,
    pub account_id: Uuid,
    pub task_list_id: Option<Uuid>,
    pub title: String,
    pub description: String,
    pub status: String,
    pub due_at: Option<String>,
    pub completed_at: Option<String>,
    pub sort_order: i32,
}

pub(crate) fn map_task_list(row: ClientTaskListRow) -> ClientTaskList {
    ClientTaskList {
        id: row.id,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        is_owned: row.is_owned,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        name: row.name,
        role: row.role,
        sort_order: row.sort_order,
        updated_at: row.updated_at,
    }
}

pub(crate) fn map_task_list_grant(row: TaskListGrantRow) -> TaskListGrant {
    TaskListGrant {
        id: row.id,
        task_list_id: row.task_list_id,
        task_list_name: row.task_list_name,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        grantee_account_id: row.grantee_account_id,
        grantee_email: row.grantee_email,
        grantee_display_name: row.grantee_display_name,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

pub(crate) fn map_task(row: ClientTaskRow) -> ClientTask {
    ClientTask {
        id: row.id,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        is_owned: row.is_owned,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        task_list_id: row.task_list_id,
        task_list_sort_order: row.task_list_sort_order,
        title: row.title,
        description: row.description,
        status: row.status,
        due_at: row.due_at,
        completed_at: row.completed_at,
        sort_order: row.sort_order,
        updated_at: row.updated_at,
    }
}

pub(crate) fn map_dav_task(row: DavTaskRow) -> DavTask {
    DavTask {
        id: row.id,
        collection_id: row.task_list_id.to_string(),
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        task_list_id: row.task_list_id,
        task_list_name: row.task_list_name,
        title: row.title,
        description: row.description,
        status: row.status,
        due_at: row.due_at,
        completed_at: row.completed_at,
        sort_order: row.sort_order,
        updated_at: row.updated_at,
    }
}

impl Storage {
    pub async fn upsert_client_task(&self, input: UpsertClientTaskInput) -> Result<ClientTask> {
        let title = input.title.trim();
        if title.is_empty() {
            bail!("task title is required");
        }

        let status = normalize_task_status(&input.status)?;
        let principal_account_id = input.principal_account_id;
        let task_id = input.id.unwrap_or_else(Uuid::new_v4);
        let existing_task = match input.id {
            Some(task_id) => self
                .fetch_client_tasks_by_ids(principal_account_id, &[task_id])
                .await?
                .into_iter()
                .next(),
            None => None,
        };
        let target_task_list = match input.task_list_id {
            Some(task_list_id) => self
                .fetch_task_lists_by_ids(principal_account_id, &[task_list_id])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("task list not found"))?,
            None => {
                if let Some(existing_task) = existing_task.as_ref() {
                    self.fetch_task_lists_by_ids(
                        principal_account_id,
                        &[existing_task.task_list_id],
                    )
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("task list not found"))?
                } else {
                    let task_lists = self.fetch_task_lists(input.account_id).await?;
                    task_lists
                        .into_iter()
                        .find(|task_list| {
                            task_list.owner_account_id == input.account_id
                                && task_list.role.as_deref() == Some(DEFAULT_TASK_LIST_ROLE)
                        })
                        .ok_or_else(|| anyhow!("default task list not found"))?
                }
            }
        };
        if !target_task_list.rights.may_write {
            bail!("write access is not granted on this task list");
        }
        if let Some(existing_task) = existing_task.as_ref() {
            if !existing_task.rights.may_write {
                bail!("write access is not granted on this task");
            }
            if existing_task.owner_account_id != target_task_list.owner_account_id {
                bail!("moving tasks across task-list owners is not supported");
            }
        }

        let owner_account_id = target_task_list.owner_account_id;
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        if owner_account_id == input.account_id {
            Self::ensure_default_task_list(&mut tx, &tenant_id, owner_account_id).await?;
        }
        let task_list_id = target_task_list.id;
        let row = sqlx::query_as::<_, ClientTaskRow>(
            r#"
            INSERT INTO tasks (
                id,
                tenant_id,
                account_id,
                task_list_id,
                title,
                description,
                status,
                due_at,
                completed_at,
                sort_order
            )
            VALUES (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                NULLIF($8, '')::timestamptz,
                CASE
                    WHEN $7 = 'completed' THEN COALESCE(NULLIF($9, '')::timestamptz, NOW())
                    ELSE NULL
                END,
                $10
            )
            ON CONFLICT (id) DO UPDATE SET
                task_list_id = EXCLUDED.task_list_id,
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                status = EXCLUDED.status,
                due_at = EXCLUDED.due_at,
                completed_at = EXCLUDED.completed_at,
                sort_order = EXCLUDED.sort_order,
                updated_at = NOW()
            WHERE tasks.tenant_id = EXCLUDED.tenant_id
              AND tasks.account_id = EXCLUDED.account_id
            RETURNING
                tasks.id,
                tasks.task_list_id,
                (
                    SELECT sort_order
                    FROM task_lists
                    WHERE task_lists.tenant_id = tasks.tenant_id
                      AND task_lists.account_id = tasks.account_id
                      AND task_lists.id = tasks.task_list_id
                ) AS task_list_sort_order,
                tasks.title,
                tasks.description,
                tasks.status,
                CASE
                    WHEN tasks.due_at IS NULL THEN NULL
                    ELSE to_char(tasks.due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS due_at,
                CASE
                    WHEN tasks.completed_at IS NULL THEN NULL
                    ELSE to_char(tasks.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS completed_at,
                tasks.sort_order,
                to_char(tasks.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            "#,
        )
        .bind(task_id)
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(task_list_id)
        .bind(title)
        .bind(input.description.trim())
        .bind(status)
        .bind(input.due_at.as_deref().unwrap_or_default().trim())
        .bind(input.completed_at.as_deref().unwrap_or_default().trim())
        .bind(input.sort_order)
        .fetch_one(&mut *tx)
        .await?;

        let mut changed_task_list_ids = vec![task_list_id];
        if let Some(existing_task) = existing_task.as_ref() {
            if existing_task.task_list_id != task_list_id {
                changed_task_list_ids.push(existing_task.task_list_id);
            }
        }
        changed_task_list_ids.sort();
        changed_task_list_ids.dedup();
        Self::emit_task_access_change(
            &mut tx,
            &tenant_id,
            owner_account_id,
            &changed_task_list_ids,
            &[],
        )
        .await?;
        tx.commit().await?;

        Ok(crate::map_task(row))
    }

    pub async fn upsert_task_list_grant(
        &self,
        input: TaskListGrantInput,
        audit: AuditEntryInput,
    ) -> Result<TaskListGrant> {
        let tenant_id = self
            .tenant_id_for_account_id(input.owner_account_id)
            .await?;
        let grantee_email = normalize_email(&input.grantee_email);
        validate_collaboration_rights(
            input.may_read,
            input.may_write,
            input.may_delete,
            input.may_share,
        )?;
        if grantee_email.is_empty() {
            bail!("grantee email is required");
        }

        let mut tx = self.pool.begin().await?;
        let owner = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, input.owner_account_id)
            .await?;
        let task_list =
            Self::load_task_list_in_tx(&mut tx, &tenant_id, owner.id, input.task_list_id).await?;
        let grantee = self
            .load_account_identity_by_email_in_tx(&mut tx, &tenant_id, &grantee_email)
            .await?;

        if owner.id == grantee.id {
            bail!("self-delegation is not supported");
        }

        sqlx::query(
            r#"
            INSERT INTO task_list_grants (
                id, tenant_id, task_list_id, owner_account_id, grantee_account_id,
                may_read, may_write, may_delete, may_share
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id, task_list_id, grantee_account_id)
            DO UPDATE SET
                may_read = EXCLUDED.may_read,
                may_write = EXCLUDED.may_write,
                may_delete = EXCLUDED.may_delete,
                may_share = EXCLUDED.may_share,
                updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(task_list.id)
        .bind(owner.id)
        .bind(grantee.id)
        .bind(input.may_read)
        .bind(input.may_write)
        .bind(input.may_delete)
        .bind(input.may_share)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_task_access_change(
            &mut tx,
            &tenant_id,
            owner.id,
            &[task_list.id],
            &[grantee.id],
        )
        .await?;
        tx.commit().await?;

        self.fetch_task_list_grant(owner.id, task_list.id, grantee.id)
            .await?
            .ok_or_else(|| anyhow!("task-list grant not found after upsert"))
    }

    pub async fn delete_task_list_grant(
        &self,
        owner_account_id: Uuid,
        task_list_id: Uuid,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM task_list_grants
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND task_list_id = $3
              AND grantee_account_id = $4
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(task_list_id)
        .bind(grantee_account_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("task-list grant not found");
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_task_access_change(
            &mut tx,
            &tenant_id,
            owner_account_id,
            &[task_list_id],
            &[grantee_account_id],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_task_list_grant(
        &self,
        owner_account_id: Uuid,
        task_list_id: Uuid,
        grantee_account_id: Uuid,
    ) -> Result<Option<TaskListGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let row = sqlx::query_as::<_, TaskListGrantRow>(
            r#"
            SELECT
                g.id,
                g.task_list_id,
                task_lists.name AS task_list_name,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.may_read,
                g.may_write,
                g.may_delete,
                g.may_share,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            FROM task_list_grants g
            JOIN task_lists
              ON task_lists.tenant_id = g.tenant_id
             AND task_lists.account_id = g.owner_account_id
             AND task_lists.id = g.task_list_id
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
              AND g.task_list_id = $3
              AND g.grantee_account_id = $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(task_list_id)
        .bind(grantee_account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(crate::map_task_list_grant))
    }

    pub async fn fetch_outgoing_task_list_grants(
        &self,
        owner_account_id: Uuid,
    ) -> Result<Vec<TaskListGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let rows = sqlx::query_as::<_, TaskListGrantRow>(
            r#"
            SELECT
                g.id,
                g.task_list_id,
                task_lists.name AS task_list_name,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.may_read,
                g.may_write,
                g.may_delete,
                g.may_share,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            FROM task_list_grants g
            JOIN task_lists
              ON task_lists.tenant_id = g.tenant_id
             AND task_lists.account_id = g.owner_account_id
             AND task_lists.id = g.task_list_id
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
            ORDER BY lower(task_lists.name) ASC, lower(grantee.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(crate::map_task_list_grant).collect())
    }

    pub async fn fetch_task_lists(&self, account_id: Uuid) -> Result<Vec<ClientTaskList>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, account_id).await?;
        let rows = sqlx::query_as::<_, ClientTaskListRow>(
            r#"
            SELECT
                task_lists.id,
                task_lists.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                (task_lists.account_id = $2) AS is_owned,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_read, FALSE)
                END AS may_read,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_write, FALSE)
                END AS may_write,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_delete, FALSE)
                END AS may_delete,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_share, FALSE)
                END AS may_share,
                task_lists.name,
                task_lists.role,
                task_lists.sort_order,
                to_char(task_lists.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            FROM task_lists
            JOIN accounts owner ON owner.id = task_lists.account_id
            LEFT JOIN task_list_grants g
              ON g.tenant_id = task_lists.tenant_id
             AND g.task_list_id = task_lists.id
             AND g.owner_account_id = task_lists.account_id
             AND g.grantee_account_id = $2
            WHERE task_lists.tenant_id = $1
              AND (task_lists.account_id = $2 OR COALESCE(g.may_read, FALSE))
            ORDER BY
                CASE WHEN task_lists.account_id = $2 THEN 0 ELSE 1 END ASC,
                lower(owner.primary_email) ASC,
                task_lists.sort_order ASC,
                task_lists.created_at ASC,
                task_lists.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.into_iter().map(crate::map_task_list).collect())
    }

    pub async fn fetch_task_lists_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientTaskList>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, account_id).await?;
        let rows = sqlx::query_as::<_, ClientTaskListRow>(
            r#"
            SELECT
                task_lists.id,
                task_lists.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                (task_lists.account_id = $2) AS is_owned,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_read, FALSE)
                END AS may_read,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_write, FALSE)
                END AS may_write,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_delete, FALSE)
                END AS may_delete,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_share, FALSE)
                END AS may_share,
                task_lists.name,
                task_lists.role,
                task_lists.sort_order,
                to_char(task_lists.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            FROM task_lists
            JOIN accounts owner ON owner.id = task_lists.account_id
            LEFT JOIN task_list_grants g
              ON g.tenant_id = task_lists.tenant_id
             AND g.task_list_id = task_lists.id
             AND g.owner_account_id = task_lists.account_id
             AND g.grantee_account_id = $2
            WHERE task_lists.tenant_id = $1
              AND task_lists.id = ANY($3)
              AND (task_lists.account_id = $2 OR COALESCE(g.may_read, FALSE))
            ORDER BY
                CASE WHEN task_lists.account_id = $2 THEN 0 ELSE 1 END ASC,
                lower(owner.primary_email) ASC,
                task_lists.sort_order ASC,
                task_lists.created_at ASC,
                task_lists.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.into_iter().map(crate::map_task_list).collect())
    }

    pub async fn create_task_list(&self, input: CreateTaskListInput) -> Result<ClientTaskList> {
        let name = normalize_task_list_name(&input.name)?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, input.account_id).await?;
        let row = sqlx::query_as::<_, ClientTaskListRow>(
            r#"
            INSERT INTO task_lists (id, tenant_id, account_id, name, role, sort_order)
            VALUES ($1, $2, $3, $4, NULL, $5)
            RETURNING
                id,
                account_id AS owner_account_id,
                ''::text AS owner_email,
                ''::text AS owner_display_name,
                TRUE AS is_owned,
                TRUE AS may_read,
                TRUE AS may_write,
                TRUE AS may_delete,
                TRUE AS may_share,
                name,
                role,
                sort_order,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(name)
        .bind(input.sort_order)
        .fetch_one(&mut *tx)
        .await?;
        Self::emit_task_access_change(&mut tx, &tenant_id, input.account_id, &[], &[]).await?;
        tx.commit().await?;

        Ok(crate::map_task_list(row))
    }

    pub async fn update_task_list(&self, input: UpdateTaskListInput) -> Result<ClientTaskList> {
        let normalized_name = input
            .name
            .as_deref()
            .map(normalize_task_list_name)
            .transpose()?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, input.account_id).await?;
        let row = sqlx::query_as::<_, ClientTaskListRow>(
            r#"
            UPDATE task_lists
            SET
                name = COALESCE($4, name),
                sort_order = COALESCE($5, sort_order),
                updated_at = CASE
                    WHEN $4 IS NULL AND $5 IS NULL THEN updated_at
                    ELSE NOW()
                END
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = $3
            RETURNING
                id,
                account_id AS owner_account_id,
                ''::text AS owner_email,
                ''::text AS owner_display_name,
                TRUE AS is_owned,
                TRUE AS may_read,
                TRUE AS may_write,
                TRUE AS may_delete,
                TRUE AS may_share,
                name,
                role,
                sort_order,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.task_list_id)
        .bind(normalized_name)
        .bind(input.sort_order)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("task list not found"))?;
        Self::emit_task_access_change(
            &mut tx,
            &tenant_id,
            input.account_id,
            &[input.task_list_id],
            &[],
        )
        .await?;
        tx.commit().await?;

        Ok(crate::map_task_list(row))
    }

    pub async fn delete_task_list(&self, account_id: Uuid, task_list_id: Uuid) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let target =
            Self::load_task_list_in_tx(&mut tx, &tenant_id, account_id, task_list_id).await?;
        if target.role.as_deref() == Some(DEFAULT_TASK_LIST_ROLE) {
            bail!("default task list cannot be destroyed");
        }
        let task_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM tasks
            WHERE tenant_id = $1 AND account_id = $2 AND task_list_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(task_list_id)
        .fetch_one(&mut *tx)
        .await?;
        if task_count > 0 {
            bail!("task list must be empty before it can be destroyed");
        }
        let grantee_account_ids = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT grantee_account_id
            FROM task_list_grants
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND task_list_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(task_list_id)
        .fetch_all(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            DELETE FROM task_lists
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(task_list_id)
        .execute(&mut *tx)
        .await?;
        Self::emit_task_access_change(
            &mut tx,
            &tenant_id,
            account_id,
            &[task_list_id],
            &grantee_account_ids,
        )
        .await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn delete_client_task(&self, account_id: Uuid, task_id: Uuid) -> Result<()> {
        let existing = self
            .fetch_client_tasks_by_ids(account_id, &[task_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("task not found"))?;
        if !existing.rights.may_delete {
            bail!("delete access is not granted on this task");
        }

        let tenant_id = self
            .tenant_id_for_account_id(existing.owner_account_id)
            .await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM tasks
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(existing.owner_account_id)
        .bind(task_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("task not found");
        }

        Self::emit_task_access_change(
            &mut tx,
            &tenant_id,
            existing.owner_account_id,
            &[existing.task_list_id],
            &[],
        )
        .await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn fetch_dav_tasks(&self, account_id: Uuid) -> Result<Vec<DavTask>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, account_id).await?;
        let rows = sqlx::query_as::<_, DavTaskRow>(
            r#"
            SELECT
                tasks.id,
                task_lists.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                (task_lists.account_id = $2) AS is_owned,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_read, FALSE)
                END AS may_read,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_write, FALSE)
                END AS may_write,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_delete, FALSE)
                END AS may_delete,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_share, FALSE)
                END AS may_share,
                tasks.task_list_id,
                task_lists.name AS task_list_name,
                tasks.title,
                tasks.description,
                tasks.status,
                CASE
                    WHEN tasks.due_at IS NULL THEN NULL
                    ELSE to_char(tasks.due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS due_at,
                CASE
                    WHEN tasks.completed_at IS NULL THEN NULL
                    ELSE to_char(tasks.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS completed_at,
                tasks.sort_order,
                to_char(tasks.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            FROM tasks
            JOIN task_lists
              ON task_lists.tenant_id = tasks.tenant_id
             AND task_lists.account_id = tasks.account_id
             AND task_lists.id = tasks.task_list_id
            JOIN accounts owner ON owner.id = task_lists.account_id
            LEFT JOIN task_list_grants g
              ON g.tenant_id = task_lists.tenant_id
             AND g.task_list_id = task_lists.id
             AND g.owner_account_id = task_lists.account_id
             AND g.grantee_account_id = $2
            WHERE tasks.tenant_id = $1
              AND (task_lists.account_id = $2 OR COALESCE(g.may_read, FALSE))
            ORDER BY
                CASE WHEN task_lists.account_id = $2 THEN 0 ELSE 1 END ASC,
                lower(owner.primary_email) ASC,
                task_lists.sort_order ASC,
                tasks.sort_order ASC,
                tasks.updated_at ASC,
                tasks.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.into_iter().map(crate::map_dav_task).collect())
    }

    pub async fn fetch_dav_tasks_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<DavTask>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, account_id).await?;
        let rows = sqlx::query_as::<_, DavTaskRow>(
            r#"
            SELECT
                tasks.id,
                task_lists.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                (task_lists.account_id = $2) AS is_owned,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_read, FALSE)
                END AS may_read,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_write, FALSE)
                END AS may_write,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_delete, FALSE)
                END AS may_delete,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_share, FALSE)
                END AS may_share,
                tasks.task_list_id,
                task_lists.name AS task_list_name,
                tasks.title,
                tasks.description,
                tasks.status,
                CASE
                    WHEN tasks.due_at IS NULL THEN NULL
                    ELSE to_char(tasks.due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS due_at,
                CASE
                    WHEN tasks.completed_at IS NULL THEN NULL
                    ELSE to_char(tasks.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS completed_at,
                tasks.sort_order,
                to_char(tasks.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            FROM tasks
            JOIN task_lists
              ON task_lists.tenant_id = tasks.tenant_id
             AND task_lists.account_id = tasks.account_id
             AND task_lists.id = tasks.task_list_id
            JOIN accounts owner ON owner.id = task_lists.account_id
            LEFT JOIN task_list_grants g
              ON g.tenant_id = task_lists.tenant_id
             AND g.task_list_id = task_lists.id
             AND g.owner_account_id = task_lists.account_id
             AND g.grantee_account_id = $2
            WHERE tasks.tenant_id = $1
              AND tasks.id = ANY($3)
              AND (task_lists.account_id = $2 OR COALESCE(g.may_read, FALSE))
            ORDER BY
                CASE WHEN task_lists.account_id = $2 THEN 0 ELSE 1 END ASC,
                lower(owner.primary_email) ASC,
                task_lists.sort_order ASC,
                tasks.sort_order ASC,
                tasks.updated_at ASC,
                tasks.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.into_iter().map(crate::map_dav_task).collect())
    }

    pub async fn upsert_dav_task(&self, input: UpsertClientTaskInput) -> Result<DavTask> {
        let task = self
            .upsert_client_task(UpsertClientTaskInput {
                task_list_id: None,
                ..input.clone()
            })
            .await?;
        let task_list_name = self
            .fetch_task_lists_by_ids(input.principal_account_id, &[task.task_list_id])
            .await?
            .into_iter()
            .next()
            .map(|task_list| task_list.name)
            .unwrap_or_default();
        Ok(DavTask {
            id: task.id,
            collection_id: task.task_list_id.to_string(),
            owner_account_id: task.owner_account_id,
            owner_email: task.owner_email,
            owner_display_name: task.owner_display_name,
            rights: task.rights,
            task_list_id: task.task_list_id,
            task_list_name,
            title: task.title,
            description: task.description,
            status: task.status,
            due_at: task.due_at,
            completed_at: task.completed_at,
            sort_order: task.sort_order,
            updated_at: task.updated_at,
        })
    }

    pub async fn delete_dav_task(&self, account_id: Uuid, task_id: Uuid) -> Result<()> {
        self.delete_client_task(account_id, task_id).await
    }

    pub async fn fetch_client_tasks(&self, account_id: Uuid) -> Result<Vec<ClientTask>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, account_id).await?;
        let rows = sqlx::query_as::<_, ClientTaskRow>(
            r#"
            SELECT
                tasks.id,
                task_lists.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                (task_lists.account_id = $2) AS is_owned,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_read, FALSE)
                END AS may_read,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_write, FALSE)
                END AS may_write,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_delete, FALSE)
                END AS may_delete,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_share, FALSE)
                END AS may_share,
                tasks.task_list_id,
                task_lists.sort_order AS task_list_sort_order,
                tasks.title,
                tasks.description,
                tasks.status,
                CASE
                    WHEN tasks.due_at IS NULL THEN NULL
                    ELSE to_char(tasks.due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS due_at,
                CASE
                    WHEN tasks.completed_at IS NULL THEN NULL
                    ELSE to_char(tasks.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS completed_at,
                tasks.sort_order,
                to_char(tasks.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            FROM tasks
            JOIN task_lists
              ON task_lists.tenant_id = tasks.tenant_id
             AND task_lists.account_id = tasks.account_id
             AND task_lists.id = tasks.task_list_id
            JOIN accounts owner ON owner.id = task_lists.account_id
            LEFT JOIN task_list_grants g
              ON g.tenant_id = task_lists.tenant_id
             AND g.task_list_id = task_lists.id
             AND g.owner_account_id = task_lists.account_id
             AND g.grantee_account_id = $2
            WHERE tasks.tenant_id = $1
              AND (task_lists.account_id = $2 OR COALESCE(g.may_read, FALSE))
            ORDER BY
                CASE WHEN task_lists.account_id = $2 THEN 0 ELSE 1 END ASC,
                lower(owner.primary_email) ASC,
                task_lists.sort_order ASC,
                tasks.sort_order ASC,
                tasks.updated_at ASC,
                tasks.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.into_iter().map(crate::map_task).collect())
    }

    pub async fn fetch_client_tasks_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientTask>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, account_id).await?;

        let rows = sqlx::query_as::<_, ClientTaskRow>(
            r#"
            SELECT
                tasks.id,
                task_lists.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                (task_lists.account_id = $2) AS is_owned,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_read, FALSE)
                END AS may_read,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_write, FALSE)
                END AS may_write,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_delete, FALSE)
                END AS may_delete,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_share, FALSE)
                END AS may_share,
                tasks.task_list_id,
                task_lists.sort_order AS task_list_sort_order,
                tasks.title,
                tasks.description,
                tasks.status,
                CASE
                    WHEN tasks.due_at IS NULL THEN NULL
                    ELSE to_char(tasks.due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS due_at,
                CASE
                    WHEN tasks.completed_at IS NULL THEN NULL
                    ELSE to_char(tasks.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                END AS completed_at,
                tasks.sort_order,
                to_char(tasks.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            FROM tasks
            JOIN task_lists
              ON task_lists.tenant_id = tasks.tenant_id
             AND task_lists.account_id = tasks.account_id
             AND task_lists.id = tasks.task_list_id
            JOIN accounts owner ON owner.id = task_lists.account_id
            LEFT JOIN task_list_grants g
              ON g.tenant_id = task_lists.tenant_id
             AND g.task_list_id = task_lists.id
             AND g.owner_account_id = task_lists.account_id
             AND g.grantee_account_id = $2
            WHERE tasks.tenant_id = $1
              AND tasks.id = ANY($3)
              AND (task_lists.account_id = $2 OR COALESCE(g.may_read, FALSE))
            ORDER BY
                CASE WHEN task_lists.account_id = $2 THEN 0 ELSE 1 END ASC,
                lower(owner.primary_email) ASC,
                task_lists.sort_order ASC,
                tasks.sort_order ASC,
                tasks.updated_at ASC,
                tasks.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.into_iter().map(crate::map_task).collect())
    }

    pub(crate) async fn ensure_default_task_list(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<ClientTaskListRow> {
        sqlx::query_as::<_, ClientTaskListRow>(
            r#"
            INSERT INTO task_lists (id, tenant_id, account_id, name, role, sort_order)
            VALUES ($1, $2, $3, $4, $5, 0)
            ON CONFLICT (tenant_id, account_id, role) DO UPDATE SET
                name = task_lists.name
            RETURNING
                id,
                account_id AS owner_account_id,
                ''::text AS owner_email,
                ''::text AS owner_display_name,
                TRUE AS is_owned,
                TRUE AS may_read,
                TRUE AS may_write,
                TRUE AS may_delete,
                TRUE AS may_share,
                name,
                role,
                sort_order,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(account_id)
        .bind(DEFAULT_TASK_LIST_NAME)
        .bind(DEFAULT_TASK_LIST_ROLE)
        .fetch_one(&mut **tx)
        .await
        .map_err(Into::into)
    }

    pub(crate) async fn load_task_list_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        task_list_id: Uuid,
    ) -> Result<ClientTaskListRow> {
        sqlx::query_as::<_, ClientTaskListRow>(
            r#"
            SELECT
                id,
                account_id AS owner_account_id,
                ''::text AS owner_email,
                ''::text AS owner_display_name,
                TRUE AS is_owned,
                TRUE AS may_read,
                TRUE AS may_write,
                TRUE AS may_delete,
                TRUE AS may_share,
                name,
                role,
                sort_order,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
            FROM task_lists
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = $3
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(task_list_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("task list not found"))
    }
}
