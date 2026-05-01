use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use std::{env, net::Ipv4Addr, path::PathBuf, time::Duration};
use tokio::{fs, process::Command, time};
use uuid::Uuid;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(20);
const SPAM_TEST_MAX_BYTES: usize = 5 * 1024 * 1024;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ServiceStatusList {
    pub(crate) items: Vec<ServiceStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ServiceStatus {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) unit: String,
    pub(crate) status: String,
    pub(crate) detail: String,
    pub(crate) action: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DiagnosticReport {
    pub(crate) title: String,
    pub(crate) status: String,
    pub(crate) detail: String,
    pub(crate) output: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ToolRunRequest {
    pub(crate) tool: String,
    pub(crate) target: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SpamTestRequest {
    pub(crate) filename: String,
    pub(crate) content_base64: String,
}

pub(crate) async fn service_statuses() -> ServiceStatusList {
    ServiceStatusList {
        items: vec![
            service_status(
                "antivirus",
                "AntiVirus",
                env_value("LPE_CT_ANTIVIRUS_SERVICE")
                    .unwrap_or_else(|| "clamav-daemon".to_string()),
            )
            .await,
            service_status(
                "lpe-ct",
                "LPE CT",
                env_value("LPE_CT_SERVICE_NAME").unwrap_or_else(|| "lpe-ct".to_string()),
            )
            .await,
        ],
    }
}

pub(crate) async fn service_action(service_id: &str, action: &str) -> Result<ServiceStatus> {
    let (name, unit) = service_definition(service_id)?;
    let action = match action {
        "start" | "stop" => action,
        _ => bail!("unsupported service action"),
    };
    let output = run_command("systemctl", &[action, &unit]).await?;
    if !output.status.success() {
        bail!("systemctl {action} {unit} failed: {}", output_text(&output));
    }
    Ok(service_status(service_id, name, unit).await)
}

pub(crate) async fn command_diagnostic(kind: &str) -> Result<DiagnosticReport> {
    match kind {
        "process-list" => {
            command_report(
                "Process List",
                "Process snapshot collected from ps.",
                "ps",
                &["-eo", "pid,ppid,stat,comm,args", "--sort=comm"],
            )
            .await
        }
        "network-connections" => {
            match command_report(
                "Network Connections",
                "Listening and connected sockets collected from ss.",
                "ss",
                &["-tulpen"],
            )
            .await
            {
                Ok(report) => Ok(report),
                Err(_) => {
                    command_report(
                        "Network Connections",
                        "Listening and connected sockets collected from netstat.",
                        "netstat",
                        &["-tulpen"],
                    )
                    .await
                }
            }
        }
        "routing-table" => routing_table_report().await,
        _ => bail!("unsupported diagnostic"),
    }
}

async fn routing_table_report() -> Result<DiagnosticReport> {
    let ip_output = run_command("ip", &["route", "show"]).await;
    match ip_output {
        Ok(output) if output.status.success() => Ok(DiagnosticReport {
            title: "Routing Table".to_string(),
            status: "ok".to_string(),
            detail: "Kernel routing table collected from ip route show.".to_string(),
            output: output_text(&output),
        }),
        Ok(output) => routing_table_from_proc(Some(output_text(&output))).await,
        Err(error) => routing_table_from_proc(Some(error.to_string())).await,
    }
}

async fn routing_table_from_proc(ip_error: Option<String>) -> Result<DiagnosticReport> {
    match fs::read_to_string("/proc/net/route").await {
        Ok(content) => Ok(DiagnosticReport {
            title: "Routing Table".to_string(),
            status: "ok".to_string(),
            detail: ip_error
                .filter(|error| !error.trim().is_empty())
                .map(|error| {
                    format!("Kernel IPv4 routing table collected from /proc/net/route because ip route show failed: {error}")
                })
                .unwrap_or_else(|| "Kernel IPv4 routing table collected from /proc/net/route.".to_string()),
            output: format_proc_ipv4_routes(&content),
        }),
        Err(error) => Ok(DiagnosticReport {
            title: "Routing Table".to_string(),
            status: "failed".to_string(),
            detail: "Routing table could not be collected from iproute2 or /proc/net/route."
                .to_string(),
            output: format!(
                "ip route show failed: {}\n/proc/net/route failed: {}",
                ip_error.unwrap_or_else(|| "unavailable".to_string()),
                error
            ),
        }),
    }
}

fn format_proc_ipv4_routes(content: &str) -> String {
    let routes = content
        .lines()
        .skip(1)
        .filter_map(format_proc_ipv4_route)
        .collect::<Vec<_>>();
    if routes.is_empty() {
        "No IPv4 routes were found in /proc/net/route.".to_string()
    } else {
        routes.join("\n")
    }
}

fn format_proc_ipv4_route(line: &str) -> Option<String> {
    let fields = line.split_whitespace().collect::<Vec<_>>();
    if fields.len() < 8 {
        return None;
    }
    let iface = fields[0];
    let destination = ipv4_from_proc_hex(fields[1])?;
    let gateway = ipv4_from_proc_hex(fields[2])?;
    let metric = fields[6].parse::<u32>().unwrap_or(0);
    let mask = ipv4_from_proc_hex(fields[7])?;
    let prefix = u32::from(mask).count_ones();
    let destination_text = if destination == Ipv4Addr::UNSPECIFIED && prefix == 0 {
        "default".to_string()
    } else {
        format!("{destination}/{prefix}")
    };
    let via = if gateway == Ipv4Addr::UNSPECIFIED {
        String::new()
    } else {
        format!(" via {gateway}")
    };
    let metric = if metric == 0 {
        String::new()
    } else {
        format!(" metric {metric}")
    };
    Some(format!("{destination_text}{via} dev {iface}{metric}"))
}

fn ipv4_from_proc_hex(value: &str) -> Option<Ipv4Addr> {
    let raw = u32::from_str_radix(value, 16).ok()?;
    let octets = raw.to_le_bytes();
    Some(Ipv4Addr::new(octets[0], octets[1], octets[2], octets[3]))
}

pub(crate) async fn run_tool(payload: ToolRunRequest) -> Result<DiagnosticReport> {
    let tool = payload.tool.trim();
    let target = payload.target.trim();
    validate_target(target)?;

    match tool {
        "ping" => {
            #[cfg(windows)]
            let args = ["-n", "4", target];
            #[cfg(not(windows))]
            let args = ["-c", "4", target];
            command_report("Ping", "ICMP reachability test.", "ping", &args).await
        }
        "traceroute" => {
            command_report(
                "Traceroute",
                "Route path diagnostic.",
                "traceroute",
                &[target],
            )
            .await
        }
        "dig" => command_report("Dig", "DNS lookup diagnostic.", "dig", &[target]).await,
        _ => bail!("unsupported diagnostic tool"),
    }
}

pub(crate) async fn support_connect() -> Result<DiagnosticReport> {
    let Ok(command) =
        configured_command("LPE_CT_SUPPORT_CONNECT_BIN", "LPE_CT_SUPPORT_CONNECT_ARGS")
    else {
        return Ok(DiagnosticReport {
            title: "LPE Support Secure Connection".to_string(),
            status: "not-configured".to_string(),
            detail: "No secure support connection command is configured.".to_string(),
            output: "Set LPE_CT_SUPPORT_CONNECT_BIN and optional LPE_CT_SUPPORT_CONNECT_ARGS to expose this operation.".to_string(),
        });
    };
    command_report(
        "LPE Support Secure Connection",
        "Support connection command executed from the configured LPE-CT host command.",
        &command.program,
        &command.args.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await
}

pub(crate) async fn spam_test(payload: SpamTestRequest) -> Result<DiagnosticReport> {
    let Ok(command) = configured_command("LPE_CT_SPAM_TEST_BIN", "LPE_CT_SPAM_TEST_ARGS") else {
        return Ok(DiagnosticReport {
            title: "Spam Test".to_string(),
            status: "not-configured".to_string(),
            detail: "No spam-test command is configured.".to_string(),
            output: "Set LPE_CT_SPAM_TEST_BIN and optional LPE_CT_SPAM_TEST_ARGS to scan uploaded test files.".to_string(),
        });
    };
    let bytes = BASE64
        .decode(payload.content_base64.trim())
        .context("uploaded spam-test file is not valid base64")?;
    if bytes.len() > SPAM_TEST_MAX_BYTES {
        bail!("uploaded spam-test file exceeds the 5 MB limit");
    }

    let safe_name = payload
        .filename
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    let path = env::temp_dir().join(format!("lpe-ct-spam-test-{}-{}", Uuid::new_v4(), safe_name));
    fs::write(&path, bytes)
        .await
        .with_context(|| format!("unable to stage spam-test upload at {}", path.display()))?;

    let mut args = command.args;
    args.push(path.to_string_lossy().to_string());
    let report = command_report(
        "Spam Test",
        "Uploaded file was passed to the configured spam-test command.",
        &command.program,
        &args.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;
    let _ = fs::remove_file(&path).await;
    report
}

pub(crate) async fn flush_mail_queue() -> Result<DiagnosticReport> {
    if let Ok(command) = configured_command(
        "LPE_CT_FLUSH_MAIL_QUEUE_BIN",
        "LPE_CT_FLUSH_MAIL_QUEUE_ARGS",
    ) {
        return command_report(
            "Flush Mail Queue",
            "Configured mail queue flush command executed.",
            &command.program,
            &command.args.iter().map(String::as_str).collect::<Vec<_>>(),
        )
        .await;
    }

    Ok(DiagnosticReport {
        title: "Flush Mail Queue".to_string(),
        status: "not-configured".to_string(),
        detail: "No external queue flush command is configured; LPE-CT internal spool retry is handled by the transport scheduler.".to_string(),
        output: "Set LPE_CT_FLUSH_MAIL_QUEUE_BIN and optional LPE_CT_FLUSH_MAIL_QUEUE_ARGS to expose a host-specific flush operation.".to_string(),
    })
}

async fn service_status(id: &str, name: &str, unit: String) -> ServiceStatus {
    let output = run_command("systemctl", &["is-active", &unit]).await;
    let (status, detail) = match output {
        Ok(output) if output.status.success() => ("running".to_string(), output_text(&output)),
        Ok(output) => {
            let detail = output_text(&output);
            let normalized = detail.lines().next().unwrap_or("").trim();
            let status = if normalized == "inactive" || normalized == "failed" {
                "not-started"
            } else {
                "unknown"
            };
            (status.to_string(), detail)
        }
        Err(error) => ("unknown".to_string(), error.to_string()),
    };
    let action = if status == "running" { "stop" } else { "start" }.to_string();
    ServiceStatus {
        id: id.to_string(),
        name: name.to_string(),
        unit,
        status,
        detail,
        action,
    }
}

fn service_definition(service_id: &str) -> Result<(&'static str, String)> {
    match service_id {
        "antivirus" => Ok((
            "AntiVirus",
            env_value("LPE_CT_ANTIVIRUS_SERVICE").unwrap_or_else(|| "clamav-daemon".to_string()),
        )),
        "lpe-ct" => Ok((
            "LPE CT",
            env_value("LPE_CT_SERVICE_NAME").unwrap_or_else(|| "lpe-ct".to_string()),
        )),
        _ => Err(anyhow!("unsupported service")),
    }
}

async fn command_report(
    title: &str,
    detail: &str,
    program: &str,
    args: &[&str],
) -> Result<DiagnosticReport> {
    let output = run_command(program, args).await?;
    Ok(DiagnosticReport {
        title: title.to_string(),
        status: if output.status.success() {
            "ok"
        } else {
            "failed"
        }
        .to_string(),
        detail: detail.to_string(),
        output: output_text(&output),
    })
}

async fn run_command(program: &str, args: &[&str]) -> Result<std::process::Output> {
    let mut command = Command::new(program);
    command.args(args);
    let child = command.output();
    time::timeout(COMMAND_TIMEOUT, child)
        .await
        .with_context(|| format!("{program} timed out"))?
        .with_context(|| format!("unable to execute {program}"))
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

fn validate_target(target: &str) -> Result<()> {
    if target.is_empty() || target.len() > 253 {
        bail!("target is required and must be at most 253 characters");
    }
    if !target
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | ':'))
    {
        bail!("target contains unsupported characters");
    }
    Ok(())
}

struct ConfiguredCommand {
    program: String,
    args: Vec<String>,
}

fn configured_command(bin_env: &str, args_env: &str) -> Result<ConfiguredCommand> {
    let program = env_value(bin_env).ok_or_else(|| anyhow!("{bin_env} is not configured"))?;
    if program.contains([' ', '\t', '\n', '\r']) || PathBuf::from(&program).is_dir() {
        bail!("{bin_env} must contain a command path or binary name without shell syntax");
    }
    let args = env_value(args_env)
        .map(|value| {
            value
                .split_whitespace()
                .filter(|part| !part.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(ConfiguredCommand { program, args })
}

fn env_value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_proc_default_route() {
        let content = "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\neth0\t00000000\t0102A8C0\t0003\t0\t0\t100\t00000000\t0\t0\t0\n";

        assert_eq!(
            format_proc_ipv4_routes(content),
            "default via 192.168.2.1 dev eth0 metric 100"
        );
    }

    #[test]
    fn formats_proc_network_route() {
        let line = "eth0\t0002A8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0";

        assert_eq!(
            format_proc_ipv4_route(line).as_deref(),
            Some("192.168.2.0/24 dev eth0")
        );
    }
}
