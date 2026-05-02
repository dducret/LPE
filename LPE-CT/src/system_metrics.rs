use serde::Serialize;
use std::{
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SystemMetrics {
    pub(crate) host_time: String,
    pub(crate) hostname: String,
    pub(crate) uptime_seconds: Option<u64>,
    pub(crate) cpu_utilization_percent: Option<f64>,
    pub(crate) processor_type: Option<String>,
    pub(crate) processor_speed_mhz: Option<f64>,
    pub(crate) os_name: Option<String>,
    pub(crate) architecture: String,
    pub(crate) memory_used_percent: Option<f64>,
    pub(crate) memory_total_bytes: Option<u64>,
    pub(crate) disk_used_percent: Option<f64>,
    pub(crate) disk_total_bytes: Option<u64>,
    pub(crate) load_averages: Option<[f64; 3]>,
    pub(crate) network_interfaces: Vec<NetworkInterfaceMetric>,
    pub(crate) dns_servers: Vec<String>,
    pub(crate) ipv4_routes: Vec<String>,
    pub(crate) ipv6_addresses: Vec<NetworkAddressMetric>,
    pub(crate) ipv6_routes: Vec<String>,
    pub(crate) ntp: NtpMetric,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NetworkInterfaceMetric {
    pub(crate) name: String,
    pub(crate) address: String,
    pub(crate) netmask: Option<String>,
    pub(crate) default_gateway: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NetworkAddressMetric {
    pub(crate) interface: String,
    pub(crate) address: String,
    pub(crate) prefix: Option<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NtpMetric {
    pub(crate) enabled: bool,
    pub(crate) servers: Vec<String>,
    pub(crate) synchronized: Option<bool>,
    pub(crate) service: Option<String>,
}

pub(crate) fn collect(spool_dir: &Path) -> SystemMetrics {
    SystemMetrics {
        host_time: host_time(),
        hostname: hostname(),
        uptime_seconds: uptime_seconds(),
        cpu_utilization_percent: cpu_utilization_percent(),
        processor_type: processor_type(),
        processor_speed_mhz: processor_speed_mhz(),
        os_name: os_name(),
        architecture: std::env::consts::ARCH.to_string(),
        memory_used_percent: memory_used_percent(),
        memory_total_bytes: memory_total_bytes(),
        disk_used_percent: disk_used_percent(spool_dir),
        disk_total_bytes: disk_total_bytes(spool_dir),
        load_averages: load_averages(),
        network_interfaces: network_interfaces(),
        dns_servers: dns_servers(),
        ipv4_routes: ip_route_lines("ip", &["-4", "route", "show"]),
        ipv6_addresses: ipv6_addresses(),
        ipv6_routes: ip_route_lines("ip", &["-6", "route", "show"]),
        ntp: ntp_metric(),
    }
}

fn host_time() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => format!("unix:{}", duration.as_secs()),
        Err(_) => "unix:0".to_string(),
    }
}

fn hostname() -> String {
    read_trimmed("/proc/sys/kernel/hostname")
        .or_else(|| env_value("HOSTNAME"))
        .or_else(|| env_value("COMPUTERNAME"))
        .unwrap_or_else(|| "localhost".to_string())
}

fn uptime_seconds() -> Option<u64> {
    let raw = read_trimmed("/proc/uptime")?;
    let first = raw.split_whitespace().next()?;
    let seconds = first.split('.').next()?;
    seconds.parse().ok()
}

fn cpu_utilization_percent() -> Option<f64> {
    let raw = read_trimmed("/proc/stat")?;
    let line = raw.lines().find(|line| line.starts_with("cpu "))?;
    let values = line
        .split_whitespace()
        .skip(1)
        .filter_map(|value| value.parse::<u64>().ok())
        .collect::<Vec<_>>();
    if values.len() < 4 {
        return None;
    }

    let idle = values.get(3).copied().unwrap_or(0) + values.get(4).copied().unwrap_or(0);
    let total: u64 = values.iter().sum();
    if total == 0 {
        return None;
    }

    Some(percent(total.saturating_sub(idle), total))
}

fn load_averages() -> Option<[f64; 3]> {
    let raw = read_trimmed("/proc/loadavg")?;
    let values = raw
        .split_whitespace()
        .take(3)
        .filter_map(|value| value.parse::<f64>().ok())
        .collect::<Vec<_>>();
    (values.len() == 3).then(|| [values[0], values[1], values[2]])
}

fn processor_type() -> Option<String> {
    cpuinfo_value("model name")
        .or_else(|| cpuinfo_value("Hardware"))
        .or_else(|| cpuinfo_value("Processor"))
}

fn processor_speed_mhz() -> Option<f64> {
    cpuinfo_value("cpu MHz").and_then(|value| value.parse().ok())
}

fn os_name() -> Option<String> {
    os_release_value("PRETTY_NAME").or_else(|| read_trimmed("/proc/sys/kernel/ostype"))
}

fn memory_total_bytes() -> Option<u64> {
    meminfo_kib("MemTotal").map(|value| value.saturating_mul(1024))
}

fn memory_used_percent() -> Option<f64> {
    let total = meminfo_kib("MemTotal")?;
    let available = meminfo_kib("MemAvailable")?;
    if total == 0 {
        return None;
    }
    Some(percent(total.saturating_sub(available), total))
}

fn cpuinfo_value(name: &str) -> Option<String> {
    key_value_file("/proc/cpuinfo", name)
}

fn meminfo_kib(name: &str) -> Option<u64> {
    key_value_file("/proc/meminfo", name).and_then(|value| {
        value
            .split_whitespace()
            .next()
            .and_then(|number| number.parse().ok())
    })
}

fn os_release_value(name: &str) -> Option<String> {
    key_value_file("/etc/os-release", name).map(|value| value.trim_matches('"').to_string())
}

fn key_value_file(path: &str, name: &str) -> Option<String> {
    let raw = fs::read_to_string(path).ok()?;
    raw.lines().find_map(|line| {
        let (key, value) = line.split_once([':', '='])?;
        if key.trim() == name {
            Some(value.trim().to_string())
        } else {
            None
        }
    })
}

fn read_trimmed(path: &str) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(unix)]
fn split_words(value: &str) -> Vec<String> {
    value
        .split_whitespace()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn percent(used: u64, total: u64) -> f64 {
    ((used as f64 / total as f64) * 10_000.0).round() / 100.0
}

#[cfg(unix)]
fn network_interfaces() -> Vec<NetworkInterfaceMetric> {
    use std::process::Command;

    let output = Command::new("ip")
        .args(["-o", "-4", "addr", "show"])
        .output()
        .ok()
        .filter(|output| output.status.success());
    let Some(output) = output else {
        return Vec::new();
    };
    let Ok(raw) = String::from_utf8(output.stdout) else {
        return Vec::new();
    };

    let gateways = default_gateways();
    raw.lines()
        .filter_map(|line| parse_ipv4_interface_line(line, &gateways))
        .collect()
}

#[cfg(not(unix))]
fn network_interfaces() -> Vec<NetworkInterfaceMetric> {
    Vec::new()
}

#[cfg(unix)]
fn dns_servers() -> Vec<String> {
    let mut servers = read_dns_servers_from_resolv_conf();
    for server in read_dns_servers_from_resolvectl() {
        if !servers.contains(&server) {
            servers.push(server);
        }
    }
    servers
}

#[cfg(not(unix))]
fn dns_servers() -> Vec<String> {
    Vec::new()
}

#[cfg(unix)]
fn read_dns_servers_from_resolv_conf() -> Vec<String> {
    fs::read_to_string("/etc/resolv.conf")
        .ok()
        .map(|raw| {
            raw.lines()
                .filter_map(|line| {
                    let line = line.split('#').next()?.trim();
                    let parts = line.split_whitespace().collect::<Vec<_>>();
                    (parts.first().copied() == Some("nameserver"))
                        .then(|| parts.get(1).copied())
                        .flatten()
                        .map(ToString::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(unix)]
fn read_dns_servers_from_resolvectl() -> Vec<String> {
    let Some(raw) = command_stdout("resolvectl", &["dns"]) else {
        return Vec::new();
    };
    raw.lines()
        .flat_map(|line| line.split_once(':').map(|(_, value)| value).unwrap_or(line).split_whitespace())
        .filter(|value| value.chars().any(|ch| ch == '.' || ch == ':'))
        .map(ToString::to_string)
        .collect()
}

#[cfg(unix)]
fn ip_route_lines(program: &str, args: &[&str]) -> Vec<String> {
    command_stdout(program, args)
        .map(|raw| {
            raw.lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(not(unix))]
fn ip_route_lines(_program: &str, _args: &[&str]) -> Vec<String> {
    Vec::new()
}

#[cfg(unix)]
fn ipv6_addresses() -> Vec<NetworkAddressMetric> {
    let Some(raw) = command_stdout("ip", &["-o", "-6", "addr", "show", "scope", "global"]) else {
        return Vec::new();
    };
    raw.lines().filter_map(parse_ipv6_address_line).collect()
}

#[cfg(not(unix))]
fn ipv6_addresses() -> Vec<NetworkAddressMetric> {
    Vec::new()
}

#[cfg(unix)]
fn parse_ipv6_address_line(line: &str) -> Option<NetworkAddressMetric> {
    let parts = line.split_whitespace().collect::<Vec<_>>();
    let raw_interface = parts.get(1)?.trim_end_matches(':');
    let interface = raw_interface.split('@').next().unwrap_or(raw_interface);
    let inet6_index = parts.iter().position(|part| *part == "inet6")?;
    let cidr = parts.get(inet6_index + 1)?;
    let (address, prefix) = cidr
        .split_once('/')
        .map(|(address, prefix)| (address, prefix.parse::<u8>().ok()))
        .unwrap_or((*cidr, None));
    Some(NetworkAddressMetric {
        interface: interface.to_string(),
        address: address.to_string(),
        prefix,
    })
}

#[cfg(unix)]
fn ntp_metric() -> NtpMetric {
    let mut enabled = false;
    let mut synchronized = None;
    let mut service = None;

    if let Some(raw) = command_stdout("timedatectl", &["show", "-p", "NTP", "-p", "NTPSynchronized"]) {
        for line in raw.lines() {
            if let Some(value) = line.strip_prefix("NTP=") {
                enabled = value.trim() == "yes";
            }
            if let Some(value) = line.strip_prefix("NTPSynchronized=") {
                synchronized = Some(value.trim() == "yes");
            }
        }
    }

    if let Some(raw) = command_stdout("systemctl", &["is-enabled", "systemd-timesyncd"]) {
        service = Some("systemd-timesyncd".to_string());
        if raw.lines().next().map(str::trim) == Some("enabled") {
            enabled = true;
        }
    }

    NtpMetric {
        enabled,
        servers: configured_ntp_servers(),
        synchronized,
        service,
    }
}

#[cfg(not(unix))]
fn ntp_metric() -> NtpMetric {
    NtpMetric {
        enabled: false,
        servers: Vec::new(),
        synchronized: None,
        service: None,
    }
}

#[cfg(unix)]
fn configured_ntp_servers() -> Vec<String> {
    let mut servers = Vec::new();
    for path in [
        "/etc/systemd/timesyncd.conf",
        "/etc/systemd/timesyncd.conf.d/lpe-ct.conf",
    ] {
        if let Ok(raw) = fs::read_to_string(path) {
            for line in raw.lines() {
                let line = line.split('#').next().unwrap_or("").trim();
                if let Some(value) = line.strip_prefix("NTP=") {
                    servers.extend(split_words(value));
                }
                if let Some(value) = line.strip_prefix("FallbackNTP=") {
                    servers.extend(split_words(value));
                }
            }
        }
    }
    servers.sort();
    servers.dedup();
    servers
}

#[cfg(unix)]
fn command_stdout(program: &str, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(unix)]
fn default_gateways() -> std::collections::BTreeMap<String, String> {
    use std::process::Command;

    let output = Command::new("ip")
        .args(["-4", "route", "show", "default"])
        .output()
        .ok()
        .filter(|output| output.status.success());
    let Some(output) = output else {
        return std::collections::BTreeMap::new();
    };
    let Ok(raw) = String::from_utf8(output.stdout) else {
        return std::collections::BTreeMap::new();
    };

    raw.lines()
        .filter_map(|line| {
            let parts = line.split_whitespace().collect::<Vec<_>>();
            let gateway = parts
                .windows(2)
                .find_map(|pair| (pair[0] == "via").then(|| pair[1].to_string()))?;
            let interface = parts
                .windows(2)
                .find_map(|pair| (pair[0] == "dev").then(|| pair[1].to_string()))?;
            Some((interface, gateway))
        })
        .collect()
}

#[cfg(unix)]
fn parse_ipv4_interface_line(
    line: &str,
    gateways: &std::collections::BTreeMap<String, String>,
) -> Option<NetworkInterfaceMetric> {
    let parts = line.split_whitespace().collect::<Vec<_>>();
    let raw_name = parts.get(1)?.trim_end_matches(':');
    let name = raw_name.split('@').next().unwrap_or(raw_name);
    if name == "lo" {
        return None;
    }

    let inet_index = parts.iter().position(|part| *part == "inet")?;
    let cidr = parts.get(inet_index + 1)?;
    let (address, prefix) = cidr.split_once('/')?;
    Some(NetworkInterfaceMetric {
        name: name.to_string(),
        address: address.to_string(),
        netmask: prefix.parse::<u8>().ok().and_then(ipv4_prefix_to_netmask),
        default_gateway: gateways.get(name).cloned(),
    })
}

#[cfg(unix)]
fn ipv4_prefix_to_netmask(prefix: u8) -> Option<String> {
    if prefix > 32 {
        return None;
    }
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    Some(format!(
        "{}.{}.{}.{}",
        (mask >> 24) & 0xff,
        (mask >> 16) & 0xff,
        (mask >> 8) & 0xff,
        mask & 0xff
    ))
}

fn disk_total_bytes(path: &Path) -> Option<u64> {
    disk_stats(path).map(|stats| stats.total_bytes)
}

fn disk_used_percent(path: &Path) -> Option<f64> {
    disk_stats(path).and_then(|stats| {
        if stats.total_bytes == 0 {
            None
        } else {
            Some(percent(
                stats.total_bytes.saturating_sub(stats.available_bytes),
                stats.total_bytes,
            ))
        }
    })
}

struct DiskStats {
    total_bytes: u64,
    available_bytes: u64,
}

#[cfg(unix)]
fn disk_stats(path: &Path) -> Option<DiskStats> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    #[repr(C)]
    struct Statvfs {
        f_bsize: u64,
        f_frsize: u64,
        f_blocks: u64,
        f_bfree: u64,
        f_bavail: u64,
        f_files: u64,
        f_ffree: u64,
        f_favail: u64,
        f_fsid: u64,
        f_flag: u64,
        f_namemax: u64,
        __f_spare: [i32; 6],
    }

    extern "C" {
        fn statvfs(path: *const i8, buf: *mut Statvfs) -> i32;
    }

    let candidate = if path.exists() {
        path
    } else {
        path.parent().unwrap_or_else(|| Path::new("/"))
    };
    let path = CString::new(candidate.as_os_str().as_bytes()).ok()?;
    let mut stats = Statvfs {
        f_bsize: 0,
        f_frsize: 0,
        f_blocks: 0,
        f_bfree: 0,
        f_bavail: 0,
        f_files: 0,
        f_ffree: 0,
        f_favail: 0,
        f_fsid: 0,
        f_flag: 0,
        f_namemax: 0,
        __f_spare: [0; 6],
    };

    let result = unsafe { statvfs(path.as_ptr(), &mut stats) };
    if result != 0 {
        return None;
    }

    let block_size = if stats.f_frsize == 0 {
        stats.f_bsize
    } else {
        stats.f_frsize
    };
    Some(DiskStats {
        total_bytes: stats.f_blocks.saturating_mul(block_size),
        available_bytes: stats.f_bavail.saturating_mul(block_size),
    })
}

#[cfg(not(unix))]
fn disk_stats(_path: &Path) -> Option<DiskStats> {
    None
}
