use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{process::Output, time::Duration};
use tokio::{io::AsyncWriteExt, process::Command, time};

const STANDARD_TIMEOUT: Duration = Duration::from_secs(30);
const APT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const HOST_ACTION_HELPER_ENV: &str = "LPE_CT_HOST_ACTION_HELPER";
const DEFAULT_HOST_ACTION_HELPER: &str = "/opt/lpe-ct/bin/lpe-ct-host-action";

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct NtpUpdateRequest {
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) servers: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SystemActionResponse {
    pub(crate) title: String,
    pub(crate) status: String,
    pub(crate) detail: String,
    pub(crate) output: String,
}

pub(crate) async fn update_ntp(payload: NtpUpdateRequest) -> Result<SystemActionResponse> {
    let servers = normalize_servers(payload.servers)?;
    if payload.enabled && servers.is_empty() {
        bail!("at least one NTP server is required when NTP is enabled");
    }

    let enabled = if payload.enabled { "true" } else { "false" };
    let output = run_host_action(
        "ntp-update",
        &[enabled],
        Some(&servers.join("\n")),
        STANDARD_TIMEOUT,
    )
    .await?;
    if !output.status.success() {
        bail!(
            "host NTP update helper failed with {}: {}",
            output.status,
            output_text(&output)
        );
    }

    Ok(SystemActionResponse {
        title: "NTP settings".to_string(),
        status: "ok".to_string(),
        detail: "Debian systemd-timesyncd configuration was updated.".to_string(),
        output: output_text(&output),
    })
}

pub(crate) async fn sync_ntp() -> Result<SystemActionResponse> {
    let output = run_host_action("ntp-sync", &[], None, STANDARD_TIMEOUT).await?;
    if !output.status.success() {
        bail!(
            "host NTP sync helper failed with {}: {}",
            output.status,
            output_text(&output)
        );
    }

    Ok(SystemActionResponse {
        title: "NTP sync".to_string(),
        status: "ok".to_string(),
        detail: "NTP synchronization was requested through systemd-timesyncd.".to_string(),
        output: output_text(&output),
    })
}

pub(crate) async fn apt_update_upgrade() -> Result<SystemActionResponse> {
    let output = run_host_action("apt-upgrade", &[], None, APT_TIMEOUT).await?;
    let update_ok = output.status.success();

    Ok(SystemActionResponse {
        title: "System updates".to_string(),
        status: if update_ok { "ok" } else { "failed" }.to_string(),
        detail: "apt update followed by apt upgrade -y completed.".to_string(),
        output: output_text(&output),
    })
}

pub(crate) async fn power_action(action: &str) -> Result<SystemActionResponse> {
    let helper_action = match action {
        "restart" => "restart",
        "shutdown" => "shutdown",
        _ => bail!("unsupported power action"),
    };
    let output = run_host_action(helper_action, &[], None, STANDARD_TIMEOUT).await?;
    if !output.status.success() {
        bail!(
            "host power helper failed with {}: {}",
            output.status,
            output_text(&output)
        );
    }

    Ok(SystemActionResponse {
        title: "Power action".to_string(),
        status: "scheduled".to_string(),
        detail: format!("{helper_action} was requested through the host action helper."),
        output: "The host may close this management session while the action is applied."
            .to_string(),
    })
}

fn normalize_servers(servers: Vec<String>) -> Result<Vec<String>> {
    let mut normalized = Vec::new();
    for value in servers {
        for server in value.split_whitespace() {
            let server = server.trim().trim_end_matches(',');
            if server.is_empty() {
                continue;
            }
            if server.len() > 253
                || !server
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | ':'))
            {
                bail!("NTP server contains unsupported characters: {server}");
            }
            let server = server.to_string();
            if !normalized.contains(&server) {
                normalized.push(server);
            }
        }
    }
    Ok(normalized)
}

async fn run_host_action(
    action: &str,
    args: &[&str],
    stdin: Option<&str>,
    timeout: Duration,
) -> Result<Output> {
    let helper = std::env::var(HOST_ACTION_HELPER_ENV)
        .unwrap_or_else(|_| DEFAULT_HOST_ACTION_HELPER.to_string());
    let mut command = Command::new("sudo");
    command.arg("-n").arg(helper).arg(action).args(args);
    if stdin.is_some() {
        command.stdin(std::process::Stdio::piped());
    }

    let mut child = command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .with_context(|| {
            "unable to execute sudo for LPE-CT host action helper; run update-lpe-ct.sh to install the helper and sudoers policy"
        })?;

    if let Some(input) = stdin {
        let mut child_stdin = child
            .stdin
            .take()
            .context("unable to open host action helper stdin")?;
        child_stdin
            .write_all(input.as_bytes())
            .await
            .context("unable to write host action helper stdin")?;
    }

    time::timeout(timeout, child.wait_with_output())
        .await
        .with_context(|| format!("host action {action} timed out"))?
        .with_context(|| format!("unable to complete host action {action}"))
}

fn output_text(output: &std::process::Output) -> String {
    let mut text = String::new();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stdout.is_empty() {
        text.push_str(&stdout);
    }
    if !stderr.is_empty() {
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        text.push_str(&stderr);
    }
    if text.is_empty() {
        format!("exit status: {}", output.status)
    } else {
        text
    }
}
