use super::*;

fn ha_role_file() -> Option<PathBuf> {
    env::var("LPE_CT_HA_ROLE_FILE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn read_ha_role() -> Result<Option<String>> {
    let Some(path) = ha_role_file() else {
        return Ok(None);
    };

    let role =
        fs::read_to_string(&path).with_context(|| format!("unable to read {}", path.display()))?;
    let normalized = role.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        anyhow::bail!("HA role file {} is empty", path.display());
    }
    if !matches!(
        normalized.as_str(),
        "active" | "standby" | "drain" | "maintenance"
    ) {
        anyhow::bail!(
            "HA role file {} contains unsupported role {}",
            path.display(),
            normalized
        );
    }

    Ok(Some(normalized))
}

pub(crate) fn ha_non_active_role_for_traffic() -> Result<Option<String>> {
    Ok(match read_ha_role()? {
        Some(role) if role != "active" => Some(role),
        _ => None,
    })
}

pub(crate) fn ha_activation_check() -> ReadinessCheck {
    match read_ha_role() {
        Ok(None) => readiness_ok(
            "ha-role",
            true,
            "HA role gating disabled; node follows default single-node readiness",
        ),
        Ok(Some(role)) if role == "active" => {
            readiness_ok("ha-role", true, "node is marked active for HA traffic")
        }
        Ok(Some(role)) => readiness_failed(
            "ha-role",
            true,
            format!("node is marked {role} and must not receive active traffic"),
        ),
        Err(error) => readiness_failed("ha-role", true, error.to_string()),
    }
}

pub(crate) fn readiness_ok(
    name: &str,
    critical: bool,
    detail: impl Into<String>,
) -> ReadinessCheck {
    ReadinessCheck {
        name: name.to_string(),
        status: "ok".to_string(),
        critical,
        detail: detail.into(),
    }
}

fn readiness_warn(name: &str, detail: impl Into<String>) -> ReadinessCheck {
    ReadinessCheck {
        name: name.to_string(),
        status: "warn".to_string(),
        critical: false,
        detail: detail.into(),
    }
}

pub(crate) fn readiness_failed(
    name: &str,
    critical: bool,
    detail: impl Into<String>,
) -> ReadinessCheck {
    ReadinessCheck {
        name: name.to_string(),
        status: "failed".to_string(),
        critical,
        detail: detail.into(),
    }
}

pub(crate) fn readiness_status(checks: &[ReadinessCheck]) -> &'static str {
    if checks
        .iter()
        .any(|check| check.critical && check.status == "failed")
    {
        "failed"
    } else {
        "ready"
    }
}

pub(crate) fn check_non_empty_value(
    name: &str,
    critical: bool,
    value: &str,
    ok_detail: &str,
    failed_detail: &str,
) -> ReadinessCheck {
    if value.trim().is_empty() {
        readiness_failed(name, critical, failed_detail)
    } else {
        readiness_ok(name, critical, ok_detail)
    }
}

pub(crate) fn check_dashboard_state_store(
    local_data_stores: &LocalDataStoresSettings,
) -> ReadinessCheck {
    if !local_data_stores.dedicated_postgres.enabled {
        return readiness_failed(
            "dashboard-state-store",
            true,
            "dashboard state requires the private LPE-CT PostgreSQL store",
        );
    }
    readiness_ok(
        "dashboard-state-store",
        true,
        "dashboard state is persisted in the private LPE-CT PostgreSQL store",
    )
}

pub(crate) fn check_spool_layout(path: &Path) -> ReadinessCheck {
    let required = smtp::SPOOL_QUEUES;
    let missing = required
        .iter()
        .map(|entry| path.join(entry))
        .filter(|entry| !entry.is_dir())
        .map(|entry| entry.display().to_string())
        .collect::<Vec<_>>();

    if missing.is_empty() {
        readiness_ok(
            "spool-layout",
            true,
            format!("required spool directories exist under {}", path.display()),
        )
    } else {
        readiness_failed(
            "spool-layout",
            true,
            format!("missing spool directories: {}", missing.join(", ")),
        )
    }
}

pub(crate) fn check_local_data_store_policy(
    local_data_stores: &LocalDataStoresSettings,
) -> ReadinessCheck {
    let dedicated_postgres = &local_data_stores.dedicated_postgres;
    if !dedicated_postgres.enabled {
        return readiness_ok(
            "local-data-stores",
            true,
            "dedicated PostgreSQL is disabled; only spool custody and state.json remain active",
        );
    }

    let Some(address) = dedicated_postgres.listen_address.as_deref() else {
        return readiness_failed(
            "local-data-stores",
            true,
            "dedicated PostgreSQL is enabled but LPE_CT_LOCAL_DB_LISTEN_ADDRESS is missing",
        );
    };

    if address_binds_publicly(address) {
        return readiness_failed(
            "local-data-stores",
            true,
            format!("dedicated PostgreSQL bind {address} is public; port 5432 must stay private"),
        );
    }

    let has_database_url = env::var("LPE_CT_LOCAL_DB_URL")
        .ok()
        .is_some_and(|value| !value.trim().is_empty());
    if !has_database_url {
        return readiness_failed(
            "local-data-stores",
            true,
            "dedicated PostgreSQL is enabled but LPE_CT_LOCAL_DB_URL is missing",
        );
    }

    let purposes = dedicated_postgres.purposes.join(", ");
    readiness_ok(
        "local-data-stores",
        true,
        format!(
            "dedicated PostgreSQL is private on {address} for purposes: {purposes} ({})",
            dedicated_postgres.network_scope
        ),
    )
}

pub(crate) fn address_binds_publicly(address: &str) -> bool {
    let normalized = address.trim();
    if matches!(
        normalized,
        "0.0.0.0" | "0.0.0.0:5432" | "::" | "[::]" | "[::]:5432"
    ) {
        return true;
    }

    if let Ok(socket) = normalized.parse::<std::net::SocketAddr>() {
        return ip_is_public(socket.ip());
    }

    let host = if normalized.starts_with('[') {
        normalized
            .strip_prefix('[')
            .and_then(|value| value.split(']').next())
            .unwrap_or(normalized)
    } else {
        normalized
            .rsplit_once(':')
            .map(|(host, _)| host)
            .unwrap_or(normalized)
    };

    if matches!(host, "0.0.0.0" | "::" | "[::]") {
        return true;
    }

    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return ip_is_public(ip);
    }

    false
}

fn ip_is_public(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => !(ip.is_loopback() || ip.is_private()),
        std::net::IpAddr::V6(ip) => !(ip.is_loopback() || ip.is_unique_local()),
    }
}

pub(crate) async fn check_optional_http_dependency(
    name: &str,
    url: &str,
    ok_detail: &str,
    warn_detail: &str,
) -> ReadinessCheck {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_millis(1_500))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return readiness_warn(
                name,
                format!("unable to initialize HTTP client for {url}: {error}"),
            );
        }
    };

    match client.get(url).send().await {
        Ok(response) if response.status().is_success() => readiness_ok(name, false, ok_detail),
        Ok(response) => readiness_warn(
            name,
            format!("{warn_detail} ({url} returned HTTP {})", response.status()),
        ),
        Err(error) => readiness_warn(name, format!("{warn_detail} ({url}: {error})")),
    }
}

pub(crate) async fn check_optional_tcp_dependency(
    name: &str,
    target: &str,
    ok_detail: &str,
    warn_detail: &str,
) -> ReadinessCheck {
    let normalized = target.trim();
    if normalized.is_empty() {
        return readiness_ok(
            name,
            false,
            "no upstream smart host configured; direct MX delivery is the default outbound mode",
        );
    }
    let address = smtp_target_socket_address(normalized);
    match tokio::time::timeout(
        Duration::from_millis(1_500),
        tokio::net::TcpStream::connect(&address),
    )
    .await
    {
        Ok(Ok(_)) => readiness_ok(name, false, ok_detail),
        Ok(Err(error)) => readiness_warn(
            name,
            format!("{warn_detail} ({normalized} -> {address}: {error})"),
        ),
        Err(_) => readiness_warn(
            name,
            format!("{warn_detail} ({normalized} -> {address}: timed out)"),
        ),
    }
}

pub(crate) fn check_spool_pressure(path: &Path) -> ReadinessCheck {
    let warn_threshold = env_u32("LPE_CT_READY_SPOOL_PRESSURE_WARN", 250);
    let deferred = count_queue_files(path, "deferred");
    let held = count_queue_files(path, "held");
    let outbound = count_queue_files(path, "outbound");
    let total = deferred + held + outbound;
    if total >= warn_threshold {
        readiness_warn(
            "spool-pressure",
            format!(
                "transport backlog is {total} message(s) across outbound={outbound}, deferred={deferred}, held={held}"
            ),
        )
    } else {
        readiness_ok(
            "spool-pressure",
            false,
            format!(
                "transport backlog is {total} message(s) across outbound={outbound}, deferred={deferred}, held={held}"
            ),
        )
    }
}

pub(crate) fn check_quarantine_backlog(path: &Path) -> ReadinessCheck {
    let warn_threshold = env_u32("LPE_CT_READY_QUARANTINE_BACKLOG_WARN", 50);
    let quarantined = count_queue_files(path, "quarantine");
    if quarantined >= warn_threshold {
        readiness_warn(
            "quarantine-backlog",
            format!("quarantine backlog is {quarantined} message(s)"),
        )
    } else {
        readiness_ok(
            "quarantine-backlog",
            false,
            format!("quarantine backlog is {quarantined} message(s)"),
        )
    }
}

fn count_queue_files(path: &Path, queue: &str) -> u32 {
    fs::read_dir(path.join(queue))
        .ok()
        .into_iter()
        .flat_map(|entries| entries.filter_map(std::result::Result::ok))
        .filter(|entry| entry.path().extension().and_then(|value| value.to_str()) == Some("json"))
        .count() as u32
}

fn env_u32(name: &str, default: u32) -> u32 {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .unwrap_or(default)
        .max(1)
}

fn smtp_target_socket_address(target: &str) -> String {
    let normalized = target
        .trim()
        .trim_start_matches("smtp://")
        .trim_start_matches("smtps://");
    if normalized.contains(':') {
        normalized.to_string()
    } else {
        format!("{normalized}:25")
    }
}

pub(crate) fn dkim_key_status(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return "not-configured".to_string();
    }
    let key_path = Path::new(trimmed);
    if !key_path.exists() {
        return "missing".to_string();
    }
    match fs::metadata(key_path) {
        Ok(metadata) if metadata.is_file() => "present".to_string(),
        Ok(_) => "invalid-path".to_string(),
        Err(_) => "unreadable".to_string(),
    }
}
