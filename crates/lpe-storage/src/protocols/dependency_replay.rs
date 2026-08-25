use anyhow::Result;
use serde_json::Value;
use uuid::Uuid;

use crate::Storage;

pub(super) async fn expand_jmap_dependency_change(
    storage: &Storage,
    tenant_id: &Uuid,
    data_type: &str,
    object_kind: &str,
    object_id: Uuid,
    summary_json: &Value,
) -> Result<Option<Vec<Uuid>>> {
    let collection_id = match object_kind {
        "contact_book" | "calendar" | "task_list" => object_id,
        "contact_book_grant" | "calendar_grant" | "task_list_grant" => {
            let Some(collection_id) = summary_json
                .get("collectionId")
                .and_then(Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok())
            else {
                return Ok(None);
            };
            collection_id
        }
        _ => return Ok(None),
    };

    let rows = match (data_type, object_kind) {
        ("ContactCard", "contact_book" | "contact_book_grant") => {
            sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id
                FROM contacts
                WHERE tenant_id = $1
                  AND contact_book_id = $2
                ORDER BY id ASC
                "#,
            )
            .bind(tenant_id)
            .bind(collection_id)
            .fetch_all(storage.pool())
            .await?
        }
        ("CalendarEvent", "calendar" | "calendar_grant") => {
            sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id
                FROM calendar_events
                WHERE tenant_id = $1
                  AND calendar_id = $2
                  AND lifecycle_state = 'active'
                  AND projection_state = 'visible'
                ORDER BY id ASC
                "#,
            )
            .bind(tenant_id)
            .bind(collection_id)
            .fetch_all(storage.pool())
            .await?
        }
        ("Task", "task_list" | "task_list_grant") => {
            sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id
                FROM tasks
                WHERE tenant_id = $1
                  AND task_list_id = $2
                ORDER BY id ASC
                "#,
            )
            .bind(tenant_id)
            .bind(collection_id)
            .fetch_all(storage.pool())
            .await?
        }
        _ => return Ok(None),
    };
    Ok(Some(rows))
}
