use super::*;

#[derive(Debug, Clone, Default)]
pub(in crate::smtp) struct DnsblOutcome {
    pub(in crate::smtp) hits: Vec<String>,
    pub(in crate::smtp) tempfail_zones: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(in crate::smtp) struct GreylistEntry {
    pub(in crate::smtp) first_seen_unix: u64,
    pub(in crate::smtp) release_after_unix: u64,
    pub(in crate::smtp) pass_count: u32,
}

pub(in crate::smtp) async fn query_dnsbl(ip: IpAddr, zones: &[String]) -> DnsblOutcome {
    let resolver = match SystemDnsResolver::new() {
        Ok(resolver) => resolver,
        Err(_) => {
            return DnsblOutcome {
                hits: Vec::new(),
                tempfail_zones: zones.to_vec(),
            };
        }
    };
    let mut outcome = DnsblOutcome::default();
    for zone in zones {
        let query = dnsbl_query_name(ip, zone);
        match resolver.query_exists(&query).await {
            Ok(true) => outcome.hits.push(zone.clone()),
            Ok(false) | Err(DnsError::NxDomain) | Err(DnsError::NoRecords) => {}
            Err(DnsError::TempFail) => outcome.tempfail_zones.push(zone.clone()),
        }
    }
    outcome
}

pub(in crate::smtp) fn dnsbl_query_name(ip: IpAddr, zone: &str) -> String {
    match ip {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            format!(
                "{}.{}.{}.{}.{}",
                octets[3], octets[2], octets[1], octets[0], zone
            )
        }
        IpAddr::V6(ip) => {
            let hex = ip
                .octets()
                .iter()
                .flat_map(|byte| [byte >> 4, byte & 0x0f])
                .map(|nibble| format!("{nibble:x}"))
                .collect::<Vec<_>>();
            format!(
                "{}.{}",
                hex.into_iter().rev().collect::<Vec<_>>().join("."),
                zone
            )
        }
    }
}

pub(in crate::smtp) async fn evaluate_greylisting(
    spool_dir: &Path,
    config: &RuntimeConfig,
    ip: IpAddr,
    mail_from: &str,
    rcpt_to: &[String],
) -> Result<Option<String>> {
    let greylist_delay_seconds = config.greylist_delay_seconds.max(1);
    let rcpt = rcpt_to.first().map(String::as_str).unwrap_or_default();
    let key = stable_key_id(&(
        ip,
        mail_from.to_ascii_lowercase(),
        rcpt.to_ascii_lowercase(),
    ));
    let now = unix_now();
    let mut entry = if let Some(pool) = ensure_local_db_schema(config).await? {
        let row = sqlx::query("SELECT state FROM greylist_entries WHERE entry_key = $1")
            .bind(&key)
            .fetch_optional(pool)
            .await?;
        row.map(|row| row.try_get::<Json<GreylistEntry>, _>("state"))
            .transpose()?
            .map(|value| value.0)
            .unwrap_or_else(|| GreylistEntry {
                first_seen_unix: now,
                release_after_unix: now + greylist_delay_seconds,
                pass_count: 0,
            })
    } else {
        let path = spool_dir.join("greylist").join(format!("{key}.json"));
        if path.exists() {
            serde_json::from_str::<GreylistEntry>(&fs::read_to_string(&path)?)?
        } else {
            GreylistEntry {
                first_seen_unix: now,
                release_after_unix: now + greylist_delay_seconds,
                pass_count: 0,
            }
        }
    };

    if now < entry.release_after_unix {
        if let Some(pool) = ensure_local_db_schema(config).await? {
            sqlx::query(
                r#"
                INSERT INTO greylist_entries (entry_key, state, updated_at)
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
        } else {
            let path = spool_dir.join("greylist").join(format!("{key}.json"));
            if !path.exists() {
                fs::write(&path, serde_json::to_string_pretty(&entry)?)?;
            }
        }
        return Ok(Some(format!(
            "greylisted triplet {} for {} seconds",
            key, greylist_delay_seconds
        )));
    }

    entry.pass_count += 1;
    if let Some(pool) = ensure_local_db_schema(config).await? {
        sqlx::query(
            r#"
            INSERT INTO greylist_entries (entry_key, state, updated_at)
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
    } else {
        let path = spool_dir.join("greylist").join(format!("{key}.json"));
        fs::write(&path, serde_json::to_string_pretty(&entry)?)?;
    }
    Ok(None)
}
