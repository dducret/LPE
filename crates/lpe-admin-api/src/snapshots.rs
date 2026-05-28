use axum::{
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use lpe_storage::{AuditEntryInput, Storage};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::task;
use uuid::Uuid;

use crate::{
    http::{bad_request_error, internal_error},
    require_admin,
    types::ApiResult,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateSnapshotRequest {
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SnapshotMetadata {
    pub id: String,
    pub label: String,
    pub created_at: String,
    pub created_by: String,
    pub dump_path: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SnapshotListResponse {
    pub snapshots: Vec<SnapshotMetadata>,
    pub snapshot_dir: String,
    pub restore_notice: String,
}

pub(crate) async fn list_snapshots(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<SnapshotListResponse> {
    require_admin(&storage, &headers, "operations").await?;
    Ok(Json(snapshot_response().map_err(internal_error)?))
}

pub(crate) async fn create_snapshot(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<CreateSnapshotRequest>,
) -> ApiResult<SnapshotListResponse> {
    let admin = require_admin(&storage, &headers, "operations").await?;
    let database_url = snapshot_database_url(&storage)?;
    let snapshot_dir = snapshot_dir();
    let label = clean_label(request.label.as_deref());
    let id = snapshot_id();
    let dump_path = snapshot_dir.join(format!("{id}.dump"));
    let metadata_path = snapshot_dir.join(format!("{id}.json"));
    let created_at = unix_timestamp();
    let created_by = admin.email.clone();
    let metadata = SnapshotMetadata {
        id: id.clone(),
        label,
        created_at,
        created_by,
        dump_path: dump_path.to_string_lossy().to_string(),
    };

    let metadata_for_file = metadata.clone();
    task::spawn_blocking(move || -> anyhow::Result<()> {
        fs::create_dir_all(&snapshot_dir)?;
        let status = Command::new(pg_tool("pg_dump"))
            .arg("--format=custom")
            .arg("--no-owner")
            .arg("--no-privileges")
            .arg("--file")
            .arg(&dump_path)
            .arg(&database_url)
            .status()?;
        if !status.success() {
            let _ = fs::remove_file(&dump_path);
            anyhow::bail!("pg_dump failed with status {status}");
        }
        let json = serde_json::to_vec_pretty(&metadata_for_file)?;
        fs::write(metadata_path, json)?;
        Ok(())
    })
    .await
    .map_err(internal_error)?
    .map_err(internal_error)?;

    storage
        .record_platform_audit(AuditEntryInput {
            actor: admin.email,
            action: "create-snapshot".to_string(),
            subject: id,
        })
        .await
        .map_err(internal_error)?;

    Ok(Json(snapshot_response().map_err(internal_error)?))
}

pub(crate) async fn delete_snapshot(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(snapshot_id): AxumPath<String>,
) -> ApiResult<SnapshotListResponse> {
    let admin = require_admin(&storage, &headers, "operations").await?;
    let snapshot = load_snapshot(&snapshot_id).map_err(snapshot_not_found)?;
    let dump_path = PathBuf::from(&snapshot.dump_path);
    let metadata_path = snapshot_dir().join(format!("{}.json", snapshot.id));

    task::spawn_blocking(move || -> anyhow::Result<()> {
        remove_if_exists(&dump_path)?;
        remove_if_exists(&metadata_path)?;
        Ok(())
    })
    .await
    .map_err(internal_error)?
    .map_err(internal_error)?;

    storage
        .record_platform_audit(AuditEntryInput {
            actor: admin.email,
            action: "delete-snapshot".to_string(),
            subject: snapshot.id,
        })
        .await
        .map_err(internal_error)?;

    Ok(Json(snapshot_response().map_err(internal_error)?))
}

pub(crate) async fn restore_snapshot(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(snapshot_id): AxumPath<String>,
) -> ApiResult<SnapshotListResponse> {
    let admin = require_admin(&storage, &headers, "operations").await?;
    let database_url = snapshot_database_url(&storage)?;
    let snapshot = load_snapshot(&snapshot_id).map_err(snapshot_not_found)?;
    let dump_path = PathBuf::from(&snapshot.dump_path);
    let subject = snapshot.id.clone();

    if !dump_path.exists() {
        return Err((
            StatusCode::NOT_FOUND,
            "snapshot dump file not found".to_string(),
        ));
    }

    task::spawn_blocking(move || -> anyhow::Result<()> {
        let status = Command::new(pg_tool("pg_restore"))
            .arg("--clean")
            .arg("--if-exists")
            .arg("--no-owner")
            .arg("--no-privileges")
            .arg("--dbname")
            .arg(&database_url)
            .arg(&dump_path)
            .status()?;
        if !status.success() {
            anyhow::bail!("pg_restore failed with status {status}");
        }
        Ok(())
    })
    .await
    .map_err(internal_error)?
    .map_err(internal_error)?;

    storage
        .record_platform_audit(AuditEntryInput {
            actor: admin.email,
            action: "restore-snapshot".to_string(),
            subject,
        })
        .await
        .map_err(internal_error)?;

    Ok(Json(snapshot_response().map_err(internal_error)?))
}

fn snapshot_database_url(storage: &Storage) -> Result<String, (StatusCode, String)> {
    storage
        .database_url()
        .map(ToString::to_string)
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .ok_or((
            StatusCode::BAD_REQUEST,
            "snapshot operations require DATABASE_URL".to_string(),
        ))
}

fn snapshot_response() -> anyhow::Result<SnapshotListResponse> {
    let dir = snapshot_dir();
    Ok(SnapshotListResponse {
        snapshots: read_snapshots(&dir)?,
        snapshot_dir: dir.to_string_lossy().to_string(),
        restore_notice: "Restoring a snapshot replaces the current PostgreSQL state; stop client traffic before restore and validate readiness afterward.".to_string(),
    })
}

fn read_snapshots(dir: &Path) -> anyhow::Result<Vec<SnapshotMetadata>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut snapshots = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let metadata = serde_json::from_slice::<SnapshotMetadata>(&fs::read(&path)?)?;
        snapshots.push(metadata);
    }
    snapshots.sort_by(|left, right| right.created_at.cmp(&left.created_at));
    Ok(snapshots)
}

fn load_snapshot(snapshot_id: &str) -> anyhow::Result<SnapshotMetadata> {
    let id = clean_snapshot_id(snapshot_id)?;
    let metadata_path = snapshot_dir().join(format!("{id}.json"));
    Ok(serde_json::from_slice::<SnapshotMetadata>(&fs::read(
        metadata_path,
    )?)?)
}

fn clean_snapshot_id(snapshot_id: &str) -> anyhow::Result<String> {
    let trimmed = snapshot_id.trim();
    if trimmed.is_empty()
        || trimmed.contains("..")
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || !trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
    {
        anyhow::bail!("invalid snapshot id");
    }
    Ok(trimmed.to_string())
}

fn snapshot_not_found(error: anyhow::Error) -> (StatusCode, String) {
    let message = error.to_string();
    if message.contains("No such file") || message.contains("not found") {
        (StatusCode::NOT_FOUND, "snapshot not found".to_string())
    } else {
        bad_request_error(error)
    }
}

fn snapshot_dir() -> PathBuf {
    std::env::var("LPE_SNAPSHOT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("tmp").join("lpe-snapshots"))
}

fn snapshot_id() -> String {
    format!("lpe-snapshot-{}-{}", unix_timestamp(), Uuid::new_v4())
}

fn unix_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

fn clean_label(label: Option<&str>) -> String {
    let trimmed = label.unwrap_or("Outlook profile test").trim();
    if trimmed.is_empty() {
        "Outlook profile test".to_string()
    } else {
        trimmed.chars().take(120).collect()
    }
}

fn pg_tool(name: &str) -> String {
    std::env::var(format!(
        "LPE_{}_PATH",
        name.to_ascii_uppercase().replace('-', "_")
    ))
    .unwrap_or_else(|_| name.to_string())
}

fn remove_if_exists(path: &Path) -> anyhow::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::{clean_label, clean_snapshot_id};

    #[test]
    fn snapshot_ids_reject_path_traversal() {
        assert!(clean_snapshot_id("../dump").is_err());
        assert!(clean_snapshot_id("nested\\dump").is_err());
        assert!(clean_snapshot_id("snapshot ok").is_err());
        assert!(clean_snapshot_id("lpe-snapshot-1-abc").is_ok());
    }

    #[test]
    fn snapshot_labels_have_stable_default() {
        assert_eq!(clean_label(None), "Outlook profile test");
        assert_eq!(clean_label(Some("  ")), "Outlook profile test");
        assert_eq!(clean_label(Some("Outlook 2019")), "Outlook 2019");
    }
}
