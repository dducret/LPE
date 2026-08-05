---
type: Rust Function
title: collect
resource: LPE-CT/src/system_metrics.rs#L54-L76
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/host_time
  - functions/LPE-CT/src/system_metrics/hostname
  - functions/LPE-CT/src/system_metrics/uptime_seconds
  - functions/LPE-CT/src/system_metrics/cpu_utilization_percent
  - functions/LPE-CT/src/system_metrics/processor_type
  - functions/LPE-CT/src/system_metrics/processor_speed_mhz
  - functions/LPE-CT/src/system_metrics/os_name
  - functions/LPE-CT/src/system_metrics/memory_used_percent
  - functions/LPE-CT/src/system_metrics/memory_total_bytes
  - functions/LPE-CT/src/system_metrics/disk_used_percent
  - functions/LPE-CT/src/system_metrics/disk_total_bytes
  - functions/LPE-CT/src/system_metrics/load_averages
---

# Signature

`pub(crate) fn collect(spool_dir: &Path) -> SystemMetrics`

# Calls

- [host_time](../../../../functions/LPE-CT/src/system_metrics/host_time.md)
- [hostname](../../../../functions/LPE-CT/src/system_metrics/hostname.md)
- [uptime_seconds](../../../../functions/LPE-CT/src/system_metrics/uptime_seconds.md)
- [cpu_utilization_percent](../../../../functions/LPE-CT/src/system_metrics/cpu_utilization_percent.md)
- [processor_type](../../../../functions/LPE-CT/src/system_metrics/processor_type.md)
- [processor_speed_mhz](../../../../functions/LPE-CT/src/system_metrics/processor_speed_mhz.md)
- [os_name](../../../../functions/LPE-CT/src/system_metrics/os_name.md)
- [memory_used_percent](../../../../functions/LPE-CT/src/system_metrics/memory_used_percent.md)
- [memory_total_bytes](../../../../functions/LPE-CT/src/system_metrics/memory_total_bytes.md)
- [disk_used_percent](../../../../functions/LPE-CT/src/system_metrics/disk_used_percent.md)
- [disk_total_bytes](../../../../functions/LPE-CT/src/system_metrics/disk_total_bytes.md)
- [load_averages](../../../../functions/LPE-CT/src/system_metrics/load_averages.md)