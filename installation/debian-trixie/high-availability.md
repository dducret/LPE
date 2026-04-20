# Debian Trixie High Availability Runbook

This runbook explains how to operate the first `LPE` / `LPE-CT` high-availability strategy on `Debian Trixie`.

## Scope

This is a pragmatic first step:

- default deployment: one `LPE`, one `LPE-CT`
- optional HA deployment: active/passive per zone
- no shared filesystem
- no application-level cluster coordinator

Recommended building blocks:

- `systemd` for process restart
- `nginx` for local publication
- `keepalived` or an equivalent network failover mechanism for VIP movement
- `PostgreSQL` primary/standby replication managed outside `LPE`

## Reference Topology

### Core zone

- `lpe-core-a`: active
- `lpe-core-b`: passive
- `pg-a`: primary
- `pg-b`: standby
- `10.20.0.40`: core VIP

### `DMZ` zone

- `lpe-ct-a`: active
- `lpe-ct-b`: passive
- `10.20.10.40`: `LPE-CT` VIP

The core nodes use:

- `DATABASE_URL` pointing to the writer endpoint
- `LPE_CT_API_BASE_URL` pointing to the `LPE-CT` VIP

The `DMZ` nodes use:

- `LPE_CT_CORE_DELIVERY_BASE_URL` pointing to the core VIP

## Health Commands

On the core node:

```bash
curl --fail http://127.0.0.1:8080/health/live
curl --fail http://127.0.0.1:8080/health/ready
/opt/lpe/src/installation/debian-trixie/check-lpe-ready.sh
```

On the `DMZ` node:

```bash
curl --fail http://127.0.0.1:8380/health/live
curl --fail http://127.0.0.1:8380/health/ready
/opt/lpe-ct/src/LPE-CT/installation/debian-trixie/check-lpe-ct-ready.sh
```

Use `/health/ready` for failover decisions. `/health/live` is only a process-level signal.

The runtime now also enforces the HA role locally:

- the core `LPE` outbound worker pauses on `standby`, `drain`, and `maintenance`
- the core inbound-delivery API rejects `LPE-CT` traffic on non-active nodes
- `LPE-CT` rejects new `SMTP` sessions and outbound handoff traffic on non-active nodes

## Keepalived Integration

The repository now provides readiness scripts intended for `keepalived` `vrrp_script` probes:

- `installation/debian-trixie/check-lpe-ready.sh`
- `LPE-CT/installation/debian-trixie/check-lpe-ct-ready.sh`

Example pattern on the core side:

```conf
vrrp_script chk_lpe_ready {
  script "/opt/lpe/src/installation/debian-trixie/check-lpe-ready.sh"
  interval 2
  fall 2
  rise 2
}
```

Example pattern on the `DMZ` side:

```conf
vrrp_script chk_lpe_ct_ready {
  script "/opt/lpe-ct/src/LPE-CT/installation/debian-trixie/check-lpe-ct-ready.sh"
  interval 2
  fall 2
  rise 2
}
```

The exact `keepalived` VRRP instance configuration depends on your network plan and is intentionally kept out of the application installer.

## Validation Scripts

On the core node:

```bash
/opt/lpe/src/installation/debian-trixie/test-ha-core-active-passive.sh
```

This validates that the local role file drives `/health/ready` correctly across `active`, `standby`, `drain`, and `maintenance`.

On the `DMZ` node:

```bash
/opt/lpe-ct/src/LPE-CT/installation/debian-trixie/test-ha-lpe-ct-active-passive.sh
```

This validates readiness, direct `SMTP` refusal on non-active roles, and `LPE -> LPE-CT` outbound-handoff refusal on non-active roles.

## Critical Components To Monitor

Monitor at least:

- `postgresql`
- `lpe.service`
- `lpe-ct.service`
- `nginx`
- `/health/ready` on both products
- free space under `/var/lib/lpe`
- free space under `/var/lib/lpe-ct`
- free space under `/var/spool/lpe-ct`
- growth of `deferred`, `held`, and `quarantine` spool queues

## Failover Procedure

### Core failover

1. Confirm the active core node is unhealthy.
2. Promote the PostgreSQL standby.
3. Move the core VIP to the passive core node.
4. Start or verify `lpe.service` and `nginx`.
5. Confirm `/health/ready` returns `"status":"ready"`.
6. Confirm outbound relay still targets the `LPE-CT` VIP.

If the former active node comes back before you are ready to reuse it, set it to `maintenance` so its inbound-delivery API and background worker stay fenced from active traffic.

### `DMZ` failover

1. Confirm the active `LPE-CT` node is unhealthy.
2. Move the `DMZ` VIP or edge publication to the passive node.
3. Start or verify `lpe-ct.service` and `nginx`.
4. Confirm `/health/ready` returns `"status":"ready"`.
5. Confirm SMTP banner and local spool writes work.
6. Confirm inbound final delivery reaches the core VIP.

Use `drain` before planned `DMZ` maintenance when you want the node to stay manageable while refusing new `SMTP` and outbound handoff traffic.

## Return To Service

When a failed node comes back:

1. keep it out of traffic first
2. update the code and configuration to the active revision
3. run the installation check script
4. inspect local persistent state before reusing it

For `LPE-CT`, explicitly inspect:

- `/var/spool/lpe-ct/deferred`
- `/var/spool/lpe-ct/held`
- `/var/spool/lpe-ct/quarantine`

Do not discard those queues blindly, because they may contain mail accepted before failover.

Tracked tooling:

```bash
/opt/lpe-ct/src/LPE-CT/installation/debian-trixie/lpe-ct-spool-recover.sh summary
/opt/lpe-ct/src/LPE-CT/installation/debian-trixie/lpe-ct-spool-recover.sh show <trace-id>
/opt/lpe-ct/src/LPE-CT/installation/debian-trixie/lpe-ct-spool-recover.sh requeue deferred all
/opt/lpe-ct/src/LPE-CT/installation/debian-trixie/lpe-ct-spool-recover.sh requeue held <trace-id>
```

The requeue helper routes traces back to `incoming` or `outbound` from their recorded `direction`. `quarantine` remains manual by design.

## Single-Node Recommendation

If you run a single `LPE` and a single `LPE-CT`, still use the new endpoints operationally:

- wire local monitoring to `/health/live` and `/health/ready`
- alert on repeated `systemd` restarts
- keep tested database and spool backups

That gives immediate value even without a standby node.
