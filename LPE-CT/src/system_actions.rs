use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{path::Path, time::Duration};
use tokio::{fs, process::Command, time};

const STANDARD_TIMEOUT: Duration = Duration::from_secs(30);
const APT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const TIMESYNCD_DROP_IN: &str = "/etc/systemd/timesyncd.conf.d/lpe-ct.conf";

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

    write_timesyncd_drop_in(&servers).await?;

    let mut output = String::new();
    if payload.enabled {
        append_command(
            &mut output,
            run_command("timedatectl", &["set-ntp", "true"], STANDARD_TIMEOUT).await?,
        );
        append_command(
            &mut output,
            run_command("systemctl", &["enable", "--now", "systemd-timesyncd"], STANDARD_TIMEOUT)
                .await?,
        );
        append_command(
            &mut output,
            run_command("systemctl", &["restart", "systemd-timesyncd"], STANDARD_TIMEOUT).await?,
        );
    } else {
        append_command(
            &mut output,
            run_command("timedatectl", &["set-ntp", "false"], STANDARD_TIMEOUT).await?,
        );
        append_command(
            &mut output,
            run_command(
                "systemctl",
                &["disable", "--now", "systemd-timesyncd"],
                STANDARD_TIMEOUT,
            )
            .await?,
        );
    }

    Ok(SystemActionResponse {
        title: "NTP settings".to_string(),
        status: "ok".to_string(),
        detail: "Debian systemd-timesyncd configuration was updated.".to_string(),
        output,
    })
}

pub(crate) async fn sync_ntp() -> Result<SystemActionResponse> {
    let mut output = String::new();
    append_command(
        &mut output,
        run_command("timedatectl", &["set-ntp", "true"], STANDARD_TIMEOUT).await?,
    );
    append_command(
        &mut output,
        run_command("systemctl", &["restart", "systemd-timesyncd"], STANDARD_TIMEOUT).await?,
    );
    if let Ok(status) = run_command("timedatectl", &["timesync-status"], STANDARD_TIMEOUT).await {
        append_command(&mut output, status);
    }

    Ok(SystemActionResponse {
        title: "NTP sync".to_string(),
        status: "ok".to_string(),
        detail: "NTP synchronization was requested through systemd-timesyncd.".to_string(),
        output,
    })
}

pub(crate) async fn apt_update_upgrade() -> Result<SystemActionResponse> {
    let mut output = String::new();
    let update = run_command("apt", &["update"], APT_TIMEOUT).await?;
    let update_ok = update.status.success();
    append_command(&mut output, update);
    if !update_ok {
        return Ok(SystemActionResponse {
            title: "System updates".to_string(),
            status: "failed".to_string(),
            detail: "apt update failed; apt upgrade was not started.".to_string(),
            output,
        });
    }

    let upgrade = run_command("apt", &["upgrade", "-y"], APT_TIMEOUT).await?;
    let upgrade_ok = upgrade.status.success();
    append_command(&mut output, upgrade);

    Ok(SystemActionResponse {
        title: "System updates".to_string(),
        status: if upgrade_ok { "ok" } else { "failed" }.to_string(),
        detail: "apt update followed by apt upgrade -y completed.".to_string(),
        output,
    })
}

pub(crate) async fn power_action(action: &str) -> Result<SystemActionResponse> {
    let systemctl_action = match action {
        "restart" => "reboot",
        "shutdown" => "poweroff",
        _ => bail!("unsupported power action"),
    };
    Command::new("systemctl")
        .arg(systemctl_action)
        .spawn()
        .with_context(|| format!("unable to execute systemctl {systemctl_action}"))?;

    Ok(SystemActionResponse {
        title: "Power action".to_string(),
        status: "scheduled".to_string(),
        detail: format!("systemctl {systemctl_action} was requested."),
        output: "The host may close this management session while the action is applied.".to_string(),
    })
}

async fn write_timesyncd_drop_in(servers: &[String]) -> Result<()> {
    let path = Path::new(TIMESYNCD_DROP_IN);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("unable to create {}", parent.display()))?;
    }
    let content = format!(
        "# Managed by LPE-CT management console.\n[Time]\nNTP={}\n",
        servers.join(" ")
    );
    fs::write(path, content)
        .await
        .with_context(|| format!("unable to write {}", path.display()))
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

async fn run_command(
    program: &str,
    args: &[&str],
    timeout: Duration,
) -> Result<std::process::Output> {
    let mut command = Command::new(program);
    command.args(args);
    if program == "apt" {
        command.env("DEBIAN_FRONTEND", "noninteractive");
    }
    time::timeout(timeout, command.output())
        .await
        .with_context(|| format!("{program} timed out"))?
        .with_context(|| format!("unable to execute {program}"))
}

fn append_command(output: &mut String, command_output: std::process::Output) {
    if !output.is_empty() {
        output.push_str("\n\n");
    }
    output.push_str(&output_text(&command_output));
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
