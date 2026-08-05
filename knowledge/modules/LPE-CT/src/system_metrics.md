---
type: Rust Module
title: system_metrics
resource: LPE-CT/src/system_metrics.rs#L1-L584
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/serde-serialize
  - external/std-fs-path-path-time-systemtime-unix-epoch
  - external/std-process-command
  - external/std-ffi-cstring-os-unix-ffi-osstrext
  member_of:
  - packages/LPE-CT
---

# Contains

- [SystemMetrics](../../../classes/LPE-CT/src/system_metrics/SystemMetrics.md)
- [NetworkInterfaceMetric](../../../classes/LPE-CT/src/system_metrics/NetworkInterfaceMetric.md)
- [NetworkAddressMetric](../../../classes/LPE-CT/src/system_metrics/NetworkAddressMetric.md)
- [NtpMetric](../../../classes/LPE-CT/src/system_metrics/NtpMetric.md)
- [collect](../../../functions/LPE-CT/src/system_metrics/collect.md)
- [host_time](../../../functions/LPE-CT/src/system_metrics/host_time.md)
- [hostname](../../../functions/LPE-CT/src/system_metrics/hostname.md)
- [uptime_seconds](../../../functions/LPE-CT/src/system_metrics/uptime_seconds.md)
- [cpu_utilization_percent](../../../functions/LPE-CT/src/system_metrics/cpu_utilization_percent.md)
- [load_averages](../../../functions/LPE-CT/src/system_metrics/load_averages.md)
- [processor_type](../../../functions/LPE-CT/src/system_metrics/processor_type.md)
- [processor_speed_mhz](../../../functions/LPE-CT/src/system_metrics/processor_speed_mhz.md)
- [os_name](../../../functions/LPE-CT/src/system_metrics/os_name.md)
- [memory_total_bytes](../../../functions/LPE-CT/src/system_metrics/memory_total_bytes.md)
- [memory_used_percent](../../../functions/LPE-CT/src/system_metrics/memory_used_percent.md)
- [cpuinfo_value](../../../functions/LPE-CT/src/system_metrics/cpuinfo_value.md)
- [meminfo_kib](../../../functions/LPE-CT/src/system_metrics/meminfo_kib.md)
- [os_release_value](../../../functions/LPE-CT/src/system_metrics/os_release_value.md)
- [key_value_file](../../../functions/LPE-CT/src/system_metrics/key_value_file.md)
- [read_trimmed](../../../functions/LPE-CT/src/system_metrics/read_trimmed.md)
- [env_value](../../../functions/LPE-CT/src/system_metrics/env_value.md)
- [split_words](../../../functions/LPE-CT/src/system_metrics/split_words.md)
- [percent](../../../functions/LPE-CT/src/system_metrics/percent.md)
- [network_interfaces](../../../functions/LPE-CT/src/system_metrics/network_interfaces.md)
- [network_interfaces](../../../functions/LPE-CT/src/system_metrics/network_interfaces-2.md)
- [dns_servers](../../../functions/LPE-CT/src/system_metrics/dns_servers.md)
- [dns_servers](../../../functions/LPE-CT/src/system_metrics/dns_servers-2.md)
- [read_dns_servers_from_resolv_conf](../../../functions/LPE-CT/src/system_metrics/read_dns_servers_from_resolv_conf.md)
- [read_dns_servers_from_resolvectl](../../../functions/LPE-CT/src/system_metrics/read_dns_servers_from_resolvectl.md)
- [ip_route_lines](../../../functions/LPE-CT/src/system_metrics/ip_route_lines.md)
- [ip_route_lines](../../../functions/LPE-CT/src/system_metrics/ip_route_lines-2.md)
- [ipv6_addresses](../../../functions/LPE-CT/src/system_metrics/ipv6_addresses.md)
- [ipv6_addresses](../../../functions/LPE-CT/src/system_metrics/ipv6_addresses-2.md)
- [parse_ipv6_address_line](../../../functions/LPE-CT/src/system_metrics/parse_ipv6_address_line.md)
- [ntp_metric](../../../functions/LPE-CT/src/system_metrics/ntp_metric.md)
- [ntp_metric](../../../functions/LPE-CT/src/system_metrics/ntp_metric-2.md)
- [configured_ntp_servers](../../../functions/LPE-CT/src/system_metrics/configured_ntp_servers.md)
- [command_stdout](../../../functions/LPE-CT/src/system_metrics/command_stdout.md)
- [default_gateways](../../../functions/LPE-CT/src/system_metrics/default_gateways.md)
- [parse_ipv4_interface_line](../../../functions/LPE-CT/src/system_metrics/parse_ipv4_interface_line.md)
- [ipv4_prefix_to_netmask](../../../functions/LPE-CT/src/system_metrics/ipv4_prefix_to_netmask.md)
- [disk_total_bytes](../../../functions/LPE-CT/src/system_metrics/disk_total_bytes.md)
- [disk_used_percent](../../../functions/LPE-CT/src/system_metrics/disk_used_percent.md)
- [DiskStats](../../../classes/LPE-CT/src/system_metrics/DiskStats.md)
- [disk_stats](../../../functions/LPE-CT/src/system_metrics/disk_stats.md)
- [Statvfs](../../../classes/LPE-CT/src/system_metrics/Statvfs.md)
- [disk_stats](../../../functions/LPE-CT/src/system_metrics/disk_stats-2.md)

# Imports

- `serde::Serialize`
- `std::{
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
}`
- `std::process::Command`
- `std::{ffi::CString, os::unix::ffi::OsStrExt}`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)