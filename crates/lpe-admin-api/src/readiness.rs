use crate::types::{ReadinessCheck, ReadinessResponse};
use std::path::PathBuf;
use std::time::Duration;

pub(crate) fn lpe_ct_base_url() -> String {
    std::env::var("LPE_CT_API_BASE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8380".to_string())
        .trim_end_matches('/')
        .to_string()
}

fn ha_role_file() -> Option<PathBuf> {
    std::env::var("LPE_HA_ROLE_FILE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn read_ha_role() -> anyhow::Result<Option<String>> {
    let Some(path) = ha_role_file() else {
        return Ok(None);
    };

    let role = std::fs::read_to_string(&path)
        .map_err(|error| anyhow::anyhow!("unable to read {}: {error}", path.display()))?;
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

pub(crate) fn readiness_warn(name: &str, detail: impl Into<String>) -> ReadinessCheck {
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

pub(crate) fn build_readiness_response(
    service: &str,
    checks: Vec<ReadinessCheck>,
) -> ReadinessResponse {
    let has_critical_failure = checks
        .iter()
        .any(|check| check.critical && check.status == "failed");
    let warnings = checks.iter().filter(|check| check.status == "warn").count() as u32;

    ReadinessResponse {
        service: service.to_string(),
        status: if has_critical_failure {
            "failed".to_string()
        } else {
            "ready".to_string()
        },
        warnings,
        checks,
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

pub fn ha_allows_active_work() -> anyhow::Result<bool> {
    match read_ha_role()? {
        None => Ok(true),
        Some(role) => Ok(role == "active"),
    }
}

pub fn ha_current_role() -> anyhow::Result<Option<String>> {
    read_ha_role()
}
