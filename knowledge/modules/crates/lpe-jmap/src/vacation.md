---
type: Rust Module
title: vacation
resource: crates/lpe-jmap/src/vacation.rs#L1-L391
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-core-sieve-action-statement
  - external/serde-deserialize
  - external/serde-json-json-map-value
  - external/std-collections-hashset
  - external/lpe-storage-auditentryinput-authenticatedaccount
  - external/crate-convert-insert-if-resolve-creation-reference-error-set-error-service-opaque-state-fingerprint-jmapservice-session-state
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [VacationResponseGetArguments](../../../../classes/crates/lpe-jmap/src/vacation/VacationResponseGetArguments.md)
- [VacationResponseSetArguments](../../../../classes/crates/lpe-jmap/src/vacation/VacationResponseSetArguments.md)
- [VacationResponseProjection](../../../../classes/crates/lpe-jmap/src/vacation/VacationResponseProjection.md)
- [handle_vacation_response_get](../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_get.md)
- [handle_vacation_response_set](../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set.md)
- [vacation_response_projection](../../../../functions/crates/lpe-jmap/src/vacation/JmapService/vacation_response_projection.md)
- [save_vacation_response](../../../../functions/crates/lpe-jmap/src/vacation/save_vacation_response.md)
- [vacation_sieve_script](../../../../functions/crates/lpe-jmap/src/vacation/vacation_sieve_script.md)
- [sieve_quote](../../../../functions/crates/lpe-jmap/src/vacation/sieve_quote.md)
- [vacation_audit](../../../../functions/crates/lpe-jmap/src/vacation/vacation_audit.md)
- [disabled](../../../../functions/crates/lpe-jmap/src/vacation/VacationResponseProjection/disabled.md)
- [find_vacation_action](../../../../functions/crates/lpe-jmap/src/vacation/find_vacation_action.md)
- [vacation_response_properties](../../../../functions/crates/lpe-jmap/src/vacation/vacation_response_properties.md)
- [vacation_response_to_value](../../../../functions/crates/lpe-jmap/src/vacation/vacation_response_to_value.md)
- [vacation_response_state](../../../../functions/crates/lpe-jmap/src/vacation/vacation_response_state.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_core::sieve::{Action, Statement}`
- `serde::Deserialize`
- `serde_json::{json, Map, Value}`
- `std::collections::HashSet`
- `lpe_storage::{AuditEntryInput, AuthenticatedAccount}`
- `crate::{
    convert::{insert_if, resolve_creation_reference},
    error::set_error,
    service::opaque_state_fingerprint,
    JmapService, SESSION_STATE,
}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)