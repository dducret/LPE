use super::*;

pub(in crate::smtp) async fn persist_message(
    spool_dir: &Path,
    queue: &str,
    message: &QueuedMessage,
) -> Result<()> {
    let destination = spool_path(spool_dir, queue, &message.id);
    let temp_path = spool_dir.join(queue).join(format!("{}.tmp", message.id));
    tokio::fs::write(&temp_path, serde_json::to_vec_pretty(message)?).await?;
    tokio::fs::rename(&temp_path, &destination).await?;
    Ok(())
}

pub(in crate::smtp) async fn move_message(
    spool_dir: &Path,
    message: &QueuedMessage,
    from: &str,
    to: &str,
) -> Result<()> {
    persist_message(spool_dir, to, message).await?;
    let _ = tokio::fs::remove_file(spool_path(spool_dir, from, &message.id)).await;
    Ok(())
}

pub(in crate::smtp) fn spool_path(spool_dir: &Path, queue: &str, id: &str) -> PathBuf {
    spool_dir.join(queue).join(format!("{id}.json"))
}

pub(in crate::smtp) struct QueueInspection {
    pub(in crate::smtp) messages: u32,
    pub(in crate::smtp) corrupt: u32,
}

pub(in crate::smtp) fn inspect_queue(spool_dir: &Path, queue: &str) -> Result<QueueInspection> {
    let path = spool_dir.join(queue);
    if !path.exists() {
        return Ok(QueueInspection {
            messages: 0,
            corrupt: 0,
        });
    }

    let mut inspection = QueueInspection {
        messages: 0,
        corrupt: 0,
    };
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        inspection.messages += 1;
        if fs::read_to_string(&path)
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .is_none()
        {
            inspection.corrupt += 1;
        }
    }

    Ok(inspection)
}

pub(in crate::smtp) fn load_message_from_path(path: &Path) -> Result<QueuedMessage> {
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

pub(in crate::smtp) fn find_message(
    spool_dir: &Path,
    trace_id: &str,
) -> Result<Option<(String, QueuedMessage)>> {
    for queue in SPOOL_QUEUES {
        let path = spool_path(spool_dir, queue, trace_id);
        if path.exists() {
            return Ok(Some((queue.to_string(), load_message_from_path(&path)?)));
        }
    }
    Ok(None)
}
