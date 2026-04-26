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

fn percent(used: u64, total: u64) -> f64 {
    ((used as f64 / total as f64) * 10_000.0).round() / 100.0
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
