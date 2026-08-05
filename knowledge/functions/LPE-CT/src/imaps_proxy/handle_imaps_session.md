---
type: Rust Function
title: handle_imaps_session
resource: LPE-CT/src/imaps_proxy.rs#L40-L62
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/readiness/ha_non_active_role_for_traffic
  - functions/crates/lpe-storage/src/core/Storage/connect
  called_by:
  - functions/LPE-CT/src/imaps_proxy/run_imaps_proxy
---

# Signature

`async fn handle_imaps_session( stream: TcpStream, _peer: SocketAddr, tls: TlsAcceptor, upstream_address: String, ) -> Result<()>`

# Calls

- [ha_non_active_role_for_traffic](../../../../functions/LPE-CT/src/readiness/ha_non_active_role_for_traffic.md)
- [connect](../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)

# Called by

- [run_imaps_proxy](../../../../functions/LPE-CT/src/imaps_proxy/run_imaps_proxy.md)