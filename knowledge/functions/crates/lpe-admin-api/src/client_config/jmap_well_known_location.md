---
type: Rust Function
title: jmap_well_known_location
resource: crates/lpe-admin-api/src/client_config.rs#L69-L71
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/from_headers
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/jmap_well_known
---

# Signature

`fn jmap_well_known_location(headers: &HeaderMap) -> String`

# Calls

- [from_headers](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/from_headers.md)

# Called by

- [jmap_well_known](../../../../../functions/crates/lpe-admin-api/src/client_config/jmap_well_known.md)