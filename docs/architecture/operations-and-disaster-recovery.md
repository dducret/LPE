# Operations and Disaster Recovery

## Current State/Functionality Overview

Operations protect canonical `LPE` PostgreSQL state, blob storage, and `LPE-CT` custody state separately. Restore and rollback must not create divergent canonical mailbox or collaboration state.

## Implementation/Usage

- Operating principles:
  - core `LPE` PostgreSQL is authoritative for user-visible state
  - `LPE-CT` local stores are technical and perimeter-owned
  - `LPE-CT` spool custody must be preserved for accepted but undelivered mail
  - readiness must pass before public routing
- Core backup boundary:
  - PostgreSQL database
  - canonical attachment/blob storage
  - configuration needed for protocol adapters and auth
  - secrets handled by the deployment secret manager
- `LPE-CT` backup boundary:
  - `/var/spool/lpe-ct`
  - quarantine payload custody
  - private technical database where enabled
  - TLS certificate/key profiles
  - routing, accepted-domain, DKIM, and policy configuration
- Restore rules:
  - restore core PostgreSQL before starting protocol adapters
  - restore blobs with database consistency
  - restore `LPE-CT` spool before accepting SMTP traffic
  - validate `/health/ready`
- Node replacement:
  - stop public routing
  - preserve or transfer `/var/spool/lpe-ct`
  - restore technical state where required
  - verify bridge connectivity to core `LPE`
  - re-enable public routing after readiness
- Upgrade safety:
  - back up before schema changes
  - verify schema compatibility
  - verify bridge signatures and integration secret presence
  - verify client autodiscovery gates after deployment

## Reference Table/List

| Item | Owner | Restore priority |
| --- | --- | --- |
| core PostgreSQL | `LPE` | highest |
| canonical blobs | `LPE` | highest |
| outbound queue | `LPE` PostgreSQL | highest |
| `LPE-CT` spool | `LPE-CT` | highest for accepted mail custody |
| quarantine metadata | `LPE-CT` | operational |
| greylisting/reputation | `LPE-CT` | operational |

| Path / command | Purpose |
| --- | --- |
| `/health/ready` | readiness check |
| `/var/spool/lpe-ct` | sorting-center spool |
| `/opt/lpe-ct/bin/lpe-ct-host-action` | host action helper |
