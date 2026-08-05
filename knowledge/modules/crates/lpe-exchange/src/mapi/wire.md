---
type: Rust Module
title: wire
resource: crates/lpe-exchange/src/mapi/wire.rs#L1-L1143
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/crate-microsoft-protocol-audit-gap-status-u16-gap-status-u32-gap-status-u8-gapstatus-fast-transfer-marker-gap-manifest-property-type-gap-manifest-rop-id-gap-manifest
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [MapiHttpRequestType](../../../../../classes/crates/lpe-exchange/src/mapi/wire/MapiHttpRequestType.md)
- [header_value](../../../../../functions/crates/lpe-exchange/src/mapi/wire/MapiHttpRequestType/header_value.md)
- [requires_nspi_session](../../../../../functions/crates/lpe-exchange/src/mapi/wire/MapiHttpRequestType/requires_nspi_session.md)
- [RopId](../../../../../classes/crates/lpe-exchange/src/mapi/wire/RopId.md)
- [as_u8](../../../../../functions/crates/lpe-exchange/src/mapi/wire/RopId/as_u8.md)
- [from_u8](../../../../../functions/crates/lpe-exchange/src/mapi/wire/RopId/from_u8.md)
- [is_reserved](../../../../../functions/crates/lpe-exchange/src/mapi/wire/RopId/is_reserved.md)
- [is_supported_by_dispatch](../../../../../functions/crates/lpe-exchange/src/mapi/wire/RopId/is_supported_by_dispatch.md)
- [known_unsupported_name](../../../../../functions/crates/lpe-exchange/src/mapi/wire/RopId/known_unsupported_name.md)
- [MapiPropertyType](../../../../../classes/crates/lpe-exchange/src/mapi/wire/MapiPropertyType.md)
- [as_u16](../../../../../functions/crates/lpe-exchange/src/mapi/wire/MapiPropertyType/as_u16.md)
- [from_code](../../../../../functions/crates/lpe-exchange/src/mapi/wire/MapiPropertyType/from_code.md)
- [known_unsupported_name](../../../../../functions/crates/lpe-exchange/src/mapi/wire/MapiPropertyType/known_unsupported_name.md)
- [MapiRestrictionType](../../../../../classes/crates/lpe-exchange/src/mapi/wire/MapiRestrictionType.md)
- [from_u8](../../../../../functions/crates/lpe-exchange/src/mapi/wire/MapiRestrictionType/from_u8.md)
- [MapiSyncType](../../../../../classes/crates/lpe-exchange/src/mapi/wire/MapiSyncType.md)
- [as_u8](../../../../../functions/crates/lpe-exchange/src/mapi/wire/MapiSyncType/as_u8.md)
- [from_u8](../../../../../functions/crates/lpe-exchange/src/mapi/wire/MapiSyncType/from_u8.md)
- [FastTransferMarker](../../../../../classes/crates/lpe-exchange/src/mapi/wire/FastTransferMarker.md)
- [as_u32](../../../../../functions/crates/lpe-exchange/src/mapi/wire/FastTransferMarker/as_u32.md)
- [from_u32](../../../../../functions/crates/lpe-exchange/src/mapi/wire/FastTransferMarker/from_u32.md)
- [known_unsupported_name](../../../../../functions/crates/lpe-exchange/src/mapi/wire/FastTransferMarker/known_unsupported_name.md)
- [MapiNotificationEventMask](../../../../../classes/crates/lpe-exchange/src/mapi/wire/MapiNotificationEventMask.md)
- [as_u16](../../../../../functions/crates/lpe-exchange/src/mapi/wire/MapiNotificationEventMask/as_u16.md)
- [MapiError](../../../../../classes/crates/lpe-exchange/src/mapi/wire/MapiError.md)
- [as_u32](../../../../../functions/crates/lpe-exchange/src/mapi/wire/MapiError/as_u32.md)
- [typed_wire_values_match_documented_constants](../../../../../functions/crates/lpe-exchange/src/mapi/wire/typed_wire_values_match_documented_constants.md)
- [typed_wire_values_decode_known_values_only](../../../../../functions/crates/lpe-exchange/src/mapi/wire/typed_wire_values_decode_known_values_only.md)

# Imports

- `crate::microsoft_protocol_audit::{
        gap_status_u16, gap_status_u32, gap_status_u8, GapStatus,
        FAST_TRANSFER_MARKER_GAP_MANIFEST, PROPERTY_TYPE_GAP_MANIFEST, ROP_ID_GAP_MANIFEST,
    }`
- `super::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)