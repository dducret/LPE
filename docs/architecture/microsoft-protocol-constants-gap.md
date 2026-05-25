# Microsoft Protocol Constants Gap Report

This report tracks Microsoft-defined protocol constants that LPE currently parses,
emits, or intentionally rejects. It is scoped to constants used by LPE protocol
surfaces, not to full Microsoft Exchange feature parity.

Status meanings:

- `Implemented`: LPE has a typed constant and parser/serializer coverage.
- `KnownUnsupported`: Microsoft defines the value, and LPE names it explicitly but
  does not implement behavior for it.
- `Reserved`: Microsoft reserves the value.
- `NotInScope`: the value is recognized as part of the protocol family, but it is
  outside LPE's current bounded interoperability surface.

| Surface | Microsoft spec | LPE manifest/test anchor | Implemented coverage | Explicit gaps |
| --- | --- | --- | --- | --- |
| ActiveSync command codes | MS-ASHTTP command codes | `crates/lpe-activesync/src/protocol.rs::activesync_command_codes_match_ms_ashttp` | `Sync`, `SendMail`, `SmartForward`, `SmartReply`, `GetAttachment`, `FolderSync`, `FolderCreate`, `FolderDelete`, `FolderUpdate`, `MoveItems`, `GetItemEstimate`, `MeetingResponse`, `Search`, `Settings`, `Ping`, `ItemOperations`, `Provision`, `ResolveRecipients`, `ValidateCert`, `Find` command-code constants are covered. | Runtime support remains bounded by the ActiveSync service handlers; `GetAttachment`, `MeetingResponse`, `Settings`, `ResolveRecipients`, and `ValidateCert` are named as known unsupported where applicable. |
| ActiveSync WBXML code pages | MS-ASWBXML code pages | `crates/lpe-activesync/src/protocol.rs::wbxml_code_pages_match_bounded_ms_aswbxml_manifest` | AirSync, Contacts, Email, Calendar, Tasks, Move, GetItemEstimate, FolderHierarchy, Ping, Provision, Search, AirSyncBase, Settings, ItemOperations, ComposeMail. | `AirNotify`, `MeetingResponse`, `ResolveRecipients`, `ValidateCert`, `Contacts2`, `GAL`, `DocumentLibrary`, `Email2`, `Notes`, `RightsManagement`, `Find`. |
| ActiveSync status/folder/body values | MS-ASCMD / MS-ASWBXML simple values | `crates/lpe-activesync/src/protocol.rs::active_sync_status_folder_and_body_values_are_manifest_checked` | Implemented status codes, folder type IDs, and body preference values are manifest-checked. | Additional command-specific status spaces are not exhaustively represented unless LPE emits them. |
| MAPIHTTP request types | MS-OXCMAPIHTTP `X-RequestType` | `crates/lpe-exchange/src/mapi/wire.rs::DOCUMENTED_SUPPORTED_VALUES` | EMSMDB and NSPI request types implemented by LPE are manifest-checked. | Unknown request types remain `Unsupported(String)` diagnostics. |
| ROP IDs | MS-OXCROPS RopId table | `crates/lpe-exchange/src/mapi/wire.rs::ROP_ID_GAP_MANIFEST` | Every `RopId::from_u8` decoded value must be classified by test. | Selected decoded values are `KnownUnsupported` or `NotInScope`, including `RopCopyTo`, `RopOpenEmbeddedMessage`, `RopSetSpooler`, `RopSpoolerLockMessage`, row expand/collapse, stream region lock/unlock, `RopTellVersion`, notify/pending, and reserved entries. |
| MAPI property types | MS-OXCDATA property data types | `crates/lpe-exchange/src/mapi/wire.rs::PROPERTY_TYPE_GAP_MANIFEST` | Implemented scalar and multivalue property types are manifest-checked. | `PtypUnspecified`, `PtypNull`, floating/currency/object/serverId/restriction/ruleAction and related multivalue forms are explicitly `KnownUnsupported`. |
| FastTransfer/ICS markers | MS-OXCFXICS markers | `crates/lpe-exchange/src/mapi/wire.rs::FAST_TRANSFER_MARKER_GAP_MANIFEST` | Incremental sync markers used by LPE are manifest-checked. | Folder/message/embed/recipient/attachment markers and partial/progress markers are explicitly `KnownUnsupported`. |
| NSPI request/property constants | MS-OXCMAPIHTTP NSPI request types and MS-OXPROPS address-book properties | `crates/lpe-exchange/src/mapi/nspi.rs::nspi_request_and_property_manifests_cover_implemented_static_values` | Supported NSPI request types, bootstrap property tags, and additional requested property tags are manifest-checked. | Common address-book properties such as `PidTagGivenName`, phone/address/title/department/location fields, and phonetic fields are explicitly `KnownUnsupported`. |
| EWS simple schema enums | EWS schema simple types | `crates/lpe-exchange/src/ews_types.rs::ews_simple_type_enums_accept_documented_mvp_values` | Delete type, distinguished folder IDs used by LPE, external audience, month, OOF state, response type, task status, and weekday values are manifest-checked. | Additional distinguished folders such as archive, recoverable items, sync issue folders, junk email, journal, notes, outbox, and related folders are explicitly `KnownUnsupported`. |

Current audit boundary:

- The manifests fail tests when LPE adds a decoded/implemented constant without
  also classifying it.
- The report does not imply full Exchange protocol parity.
- Behavior is intentionally unchanged by the manifests.

