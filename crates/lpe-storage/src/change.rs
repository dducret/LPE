use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgListener, Postgres};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::{CollaborationResourceKind, Storage, CANONICAL_CHANGE_CHANNEL};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CanonicalChangeCategory {
    Mail,
    Contacts,
    Calendar,
    Tasks,
}

impl CanonicalChangeCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mail => "mail",
            Self::Contacts => "contacts",
            Self::Calendar => "calendar",
            Self::Tasks => "tasks",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "mail" => Some(Self::Mail),
            "contacts" => Some(Self::Contacts),
            "calendar" => Some(Self::Calendar),
            "tasks" => Some(Self::Tasks),
            _ => None,
        }
    }
}

pub struct CanonicalChangeListener {
    principal_account_id: Uuid,
    tenant_id: String,
    listener: PgListener,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CanonicalPushChangeSet {
    scoped_accounts: HashMap<CanonicalChangeCategory, HashSet<Uuid>>,
    journal_cursor: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CanonicalChangeReplay {
    pub change_set: CanonicalPushChangeSet,
    pub current_cursor: Option<i64>,
    pub truncated: bool,
}

impl CanonicalPushChangeSet {
    pub fn is_empty(&self) -> bool {
        self.scoped_accounts.values().all(HashSet::is_empty)
    }

    pub fn insert_accounts<I>(&mut self, category: CanonicalChangeCategory, account_ids: I)
    where
        I: IntoIterator<Item = Uuid>,
    {
        self.scoped_accounts
            .entry(category)
            .or_default()
            .extend(account_ids);
    }

    pub fn accounts_for(&self, category: CanonicalChangeCategory) -> HashSet<Uuid> {
        self.scoped_accounts
            .get(&category)
            .cloned()
            .unwrap_or_default()
    }

    pub fn contains_category(&self, category: CanonicalChangeCategory) -> bool {
        self.scoped_accounts
            .get(&category)
            .is_some_and(|accounts| !accounts.is_empty())
    }

    pub fn set_journal_cursor(&mut self, journal_cursor: i64) {
        self.journal_cursor = Some(journal_cursor);
    }

    pub fn journal_cursor(&self) -> Option<i64> {
        self.journal_cursor
    }
}

impl CanonicalChangeListener {
    pub async fn wait_for_change(
        &mut self,
        categories: &[CanonicalChangeCategory],
    ) -> Result<CanonicalPushChangeSet> {
        let categories = categories.iter().copied().collect::<HashSet<_>>();
        if categories.is_empty() {
            return Ok(CanonicalPushChangeSet::default());
        }

        loop {
            let notification = self.listener.recv().await?;
            let Ok(payload) =
                serde_json::from_str::<CanonicalChangeNotification>(notification.payload())
            else {
                continue;
            };
            if payload.tenant_id != self.tenant_id {
                continue;
            }

            let Some(category) = CanonicalChangeCategory::from_str(&payload.category) else {
                continue;
            };
            if !categories.contains(&category) {
                continue;
            }
            if !payload
                .principal_account_ids
                .iter()
                .any(|value| value == &self.principal_account_id.to_string())
            {
                continue;
            }

            let mut changes = CanonicalPushChangeSet::default();
            changes.insert_accounts(
                category,
                payload
                    .account_ids
                    .iter()
                    .filter_map(|value| Uuid::parse_str(value).ok()),
            );
            changes.set_journal_cursor(payload.journal_sequence);
            if !changes.is_empty() {
                return Ok(changes);
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct CanonicalChangeNotification {
    tenant_id: String,
    category: String,
    journal_sequence: i64,
    principal_account_ids: Vec<String>,
    account_ids: Vec<String>,
}

impl Storage {
    pub async fn create_canonical_change_listener(
        &self,
        principal_account_id: Uuid,
    ) -> Result<CanonicalChangeListener> {
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(CANONICAL_CHANGE_CHANNEL).await?;
        Ok(CanonicalChangeListener {
            principal_account_id,
            tenant_id,
            listener,
        })
    }

    pub(crate) async fn emit_canonical_change(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        category: CanonicalChangeCategory,
        principal_account_ids: &[Uuid],
        account_ids: &[Uuid],
    ) -> Result<()> {
        let principal_account_ids = dedup_sorted_uuids(principal_account_ids);
        let account_ids = dedup_sorted_uuids(account_ids);
        let journal_sequence = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO canonical_change_journal (
                tenant_id,
                category,
                principal_account_ids,
                account_ids
            )
            VALUES ($1, $2, $3, $4)
            RETURNING sequence
            "#,
        )
        .bind(tenant_id)
        .bind(category.as_str())
        .bind(&principal_account_ids)
        .bind(&account_ids)
        .fetch_one(&mut **tx)
        .await?;
        let payload = serde_json::to_string(&CanonicalChangeNotification {
            tenant_id: tenant_id.to_string(),
            category: category.as_str().to_string(),
            journal_sequence,
            principal_account_ids: principal_account_ids.iter().map(Uuid::to_string).collect(),
            account_ids: account_ids.iter().map(Uuid::to_string).collect(),
        })?;
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(CANONICAL_CHANGE_CHANNEL)
            .bind(payload)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    pub(crate) async fn emit_mail_change(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<()> {
        let mut principal_account_ids = HashSet::from([account_id]);
        let delegated_account_ids = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT grantee_account_id
            FROM mailbox_delegation_grants
            WHERE tenant_id = $1
              AND owner_account_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_all(&mut **tx)
        .await?;
        principal_account_ids.extend(delegated_account_ids);

        let mut principal_account_ids = principal_account_ids.into_iter().collect::<Vec<_>>();
        principal_account_ids.sort();

        Self::emit_canonical_change(
            tx,
            tenant_id,
            CanonicalChangeCategory::Mail,
            &principal_account_ids,
            &[account_id],
        )
        .await
    }

    pub(crate) async fn emit_mail_delegation_change(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
    ) -> Result<()> {
        let mut principal_account_ids = vec![owner_account_id, grantee_account_id];
        principal_account_ids.sort();
        principal_account_ids.dedup();
        Self::emit_canonical_change(
            tx,
            tenant_id,
            CanonicalChangeCategory::Mail,
            &principal_account_ids,
            &[owner_account_id],
        )
        .await
    }

    pub(crate) async fn emit_collaboration_change(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        category: CanonicalChangeCategory,
        owner_account_id: Uuid,
    ) -> Result<()> {
        let collection_kind = match category {
            CanonicalChangeCategory::Contacts => CollaborationResourceKind::Contacts,
            CanonicalChangeCategory::Calendar => CollaborationResourceKind::Calendar,
            CanonicalChangeCategory::Tasks => CollaborationResourceKind::Tasks,
            _ => bail!("unsupported collaboration change category"),
        };

        let mut principal_account_ids = HashSet::from([owner_account_id]);
        let shared_with = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT grantee_account_id
            FROM collaboration_collection_grants
            WHERE tenant_id = $1
              AND collection_kind = $2
              AND owner_account_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(collection_kind.as_str())
        .bind(owner_account_id)
        .fetch_all(&mut **tx)
        .await?;
        principal_account_ids.extend(shared_with);

        let mut principal_account_ids = principal_account_ids.into_iter().collect::<Vec<_>>();
        principal_account_ids.sort();

        Self::emit_canonical_change(
            tx,
            tenant_id,
            category,
            &principal_account_ids,
            &principal_account_ids,
        )
        .await
    }

    pub(crate) async fn emit_collaboration_grant_change(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        kind: CollaborationResourceKind,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
    ) -> Result<()> {
        let category = match kind {
            CollaborationResourceKind::Contacts => CanonicalChangeCategory::Contacts,
            CollaborationResourceKind::Calendar => CanonicalChangeCategory::Calendar,
            CollaborationResourceKind::Tasks => CanonicalChangeCategory::Tasks,
        };
        let mut principal_account_ids = vec![owner_account_id, grantee_account_id];
        principal_account_ids.sort();
        principal_account_ids.dedup();

        Self::emit_canonical_change(
            tx,
            tenant_id,
            category,
            &principal_account_ids,
            &principal_account_ids,
        )
        .await
    }

    pub(crate) async fn emit_task_access_change(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        owner_account_id: Uuid,
        task_list_ids: &[Uuid],
        extra_principal_account_ids: &[Uuid],
    ) -> Result<()> {
        let mut principal_account_ids = HashSet::from([owner_account_id]);
        principal_account_ids.extend(extra_principal_account_ids.iter().copied());

        let granted_account_ids = if task_list_ids.is_empty() {
            Vec::new()
        } else {
            sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT grantee_account_id
                FROM task_list_grants
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND task_list_id = ANY($3)
                "#,
            )
            .bind(tenant_id)
            .bind(owner_account_id)
            .bind(task_list_ids)
            .fetch_all(&mut **tx)
            .await?
        };
        principal_account_ids.extend(granted_account_ids);
        let mut principal_account_ids = principal_account_ids.into_iter().collect::<Vec<_>>();
        principal_account_ids.sort();
        principal_account_ids.dedup();
        Self::emit_canonical_change(
            tx,
            tenant_id,
            CanonicalChangeCategory::Tasks,
            &principal_account_ids,
            &principal_account_ids,
        )
        .await
    }

    pub async fn fetch_canonical_change_cursor(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Option<i64>> {
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT MAX(sequence)
            FROM canonical_change_journal
            WHERE tenant_id = $1
            "#,
        )
        .bind(tenant_id)
        .fetch_one(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn replay_canonical_changes(
        &self,
        principal_account_id: Uuid,
        after_cursor: i64,
        categories: &[CanonicalChangeCategory],
        max_rows: u64,
    ) -> Result<CanonicalChangeReplay> {
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let current_cursor = self
            .fetch_canonical_change_cursor(principal_account_id)
            .await?;
        if categories.is_empty() {
            return Ok(CanonicalChangeReplay {
                change_set: CanonicalPushChangeSet::default(),
                current_cursor,
                truncated: false,
            });
        }

        let category_names = categories
            .iter()
            .map(|category| category.as_str())
            .collect::<Vec<_>>();
        let rows = sqlx::query_as::<_, CanonicalChangeJournalRow>(
            r#"
            SELECT sequence, category, account_ids
            FROM canonical_change_journal
            WHERE tenant_id = $1
              AND sequence > $2
              AND category = ANY($3)
              AND principal_account_ids @> ARRAY[$4]::uuid[]
            ORDER BY sequence ASC
            LIMIT $5
            "#,
        )
        .bind(&tenant_id)
        .bind(after_cursor)
        .bind(&category_names)
        .bind(principal_account_id)
        .bind((max_rows + 1) as i64)
        .fetch_all(&self.pool)
        .await?;

        let truncated = rows.len() > max_rows as usize;
        let rows = rows.into_iter().take(max_rows as usize).collect::<Vec<_>>();
        let mut change_set = CanonicalPushChangeSet::default();
        for row in &rows {
            let Some(category) = CanonicalChangeCategory::from_str(&row.category) else {
                continue;
            };
            change_set.insert_accounts(category, row.account_ids.iter().copied());
            change_set.set_journal_cursor(row.sequence);
        }

        Ok(CanonicalChangeReplay {
            change_set,
            current_cursor,
            truncated,
        })
    }
}

#[derive(sqlx::FromRow)]
struct CanonicalChangeJournalRow {
    sequence: i64,
    category: String,
    account_ids: Vec<Uuid>,
}

fn dedup_sorted_uuids(values: &[Uuid]) -> Vec<Uuid> {
    let mut values = values.to_vec();
    values.sort();
    values.dedup();
    values
}
