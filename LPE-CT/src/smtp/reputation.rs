use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ReputationStore {
    entries: HashMap<String, ReputationEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ReputationEntry {
    accepted: u32,
    quarantined: u32,
    rejected: u32,
    deferred: u32,
}

pub(in crate::smtp) async fn load_reputation_score(
    spool_dir: &Path,
    config: &RuntimeConfig,
    peer_ip: Option<IpAddr>,
    mail_from: &str,
) -> Result<i32> {
    let key = reputation_key(peer_ip, mail_from);
    let entry = if let Some(pool) = ensure_local_db_schema(config).await? {
        let row = sqlx::query("SELECT state FROM reputation_entries WHERE entry_key = $1")
            .bind(&key)
            .fetch_optional(pool)
            .await?;
        row.map(|row| row.try_get::<Json<ReputationEntry>, _>("state"))
            .transpose()?
            .map(|value| value.0)
            .unwrap_or_default()
    } else {
        let store = load_reputation_store(spool_dir)?;
        store.entries.get(&key).cloned().unwrap_or_default()
    };
    Ok(entry.accepted as i32
        - entry.deferred as i32
        - (entry.quarantined as i32 * 2)
        - (entry.rejected as i32 * 3))
}

pub(in crate::smtp) async fn update_reputation(
    spool_dir: &Path,
    config: &RuntimeConfig,
    message: &QueuedMessage,
    action: FilterAction,
) -> Result<()> {
    let key = reputation_key(parse_peer_ip(&message.peer), &message.mail_from);
    let mut entry = if let Some(pool) = ensure_local_db_schema(config).await? {
        let row = sqlx::query("SELECT state FROM reputation_entries WHERE entry_key = $1")
            .bind(&key)
            .fetch_optional(pool)
            .await?;
        row.map(|row| row.try_get::<Json<ReputationEntry>, _>("state"))
            .transpose()?
            .map(|value| value.0)
            .unwrap_or_default()
    } else {
        let store = load_reputation_store(spool_dir)?;
        store.entries.get(&key).cloned().unwrap_or_default()
    };
    match action {
        FilterAction::Accept => entry.accepted += 1,
        FilterAction::Quarantine => entry.quarantined += 1,
        FilterAction::Reject => entry.rejected += 1,
        FilterAction::Defer => entry.deferred += 1,
    }
    if let Some(pool) = ensure_local_db_schema(config).await? {
        sqlx::query(
            r#"
            INSERT INTO reputation_entries (entry_key, state, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (entry_key) DO UPDATE SET
                state = EXCLUDED.state,
                updated_at = NOW()
            "#,
        )
        .bind(&key)
        .bind(Json(&entry))
        .execute(pool)
        .await?;
        Ok(())
    } else {
        let mut store = load_reputation_store(spool_dir)?;
        store.entries.insert(key, entry);
        save_reputation_store(spool_dir, &store)
    }
}

fn reputation_key(peer_ip: Option<IpAddr>, mail_from: &str) -> String {
    format!(
        "{}|{}",
        peer_ip
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        sender_domain(mail_from)
    )
}

fn sender_domain(mail_from: &str) -> String {
    mail_from
        .split('@')
        .nth(1)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn load_reputation_store(spool_dir: &Path) -> Result<ReputationStore> {
    let path = spool_dir.join("policy").join("reputation.json");
    if !path.exists() {
        return Ok(ReputationStore::default());
    }
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

fn save_reputation_store(spool_dir: &Path, store: &ReputationStore) -> Result<()> {
    let path = spool_dir.join("policy").join("reputation.json");
    fs::write(path, serde_json::to_string_pretty(store)?)?;
    Ok(())
}
