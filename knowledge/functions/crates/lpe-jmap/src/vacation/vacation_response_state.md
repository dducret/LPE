---
type: Rust Function
title: vacation_response_state
resource: crates/lpe-jmap/src/vacation.rs#L386-L391
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_get
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set
---

# Signature

`fn vacation_response_state(projection: &VacationResponseProjection) -> String`

# Calls

- [opaque_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)

# Called by

- [handle_vacation_response_get](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_get.md)
- [handle_vacation_response_set](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set.md)