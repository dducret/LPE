# MAPI Receive-Folder Routing

This note defines the current `RopGetReceiveFolder`,
`RopGetReceiveFolderTable`, and `RopSetReceiveFolder` boundary for MAPI over
HTTP. It is documentation-only and does not widen endpoint publication.

## Current Behavior

`RopGetReceiveFolder` resolves message classes through a fixed canonical map:

| Message class | Canonical folder |
| --- | --- |
| `IPM` | Inbox |
| `IPM.Note` | Inbox |
| `IPM.Appointment` and `IPM.Appointment.*` | Calendar |
| `IPM.Contact` and `IPM.Contact.*` | Contacts |

The lookup uses the same longest-prefix behavior as the receive-folder table
projection. `RopGetReceiveFolderTable` exposes that fixed map from the private
mailbox logon handle and keeps the Calendar row before the generic `IPM` row so
Outlook can bind appointment objects to the advertised Calendar folder.

`RopSetReceiveFolder` is intentionally bounded. It accepts only writes that
confirm the same canonical map already advertised by LPE. A request such as
`IPM.Appointment.Custom` pointing at the canonical Calendar FID succeeds because
it does not create new routing state. A request that points a message class at a
different folder is rejected with a ROP-specific invalid-parameter response.

Receive-folder ROPs are valid only on the private mailbox logon handle. Folder
handles, public-folder logons, missing message classes, malformed folder IDs,
and unsupported message classes fail without canonical state changes.

## Boundary

The current implementation is compatibility acknowledgement, not configurable
delivery routing. LPE does not store protocol-local receive-folder assignments
and does not let Outlook redirect canonical delivery targets through MAPI.

The canonical model remains:

- Inbox receives canonical mail delivery.
- Calendar receives canonical calendar items.
- Contacts receives canonical contact items.
- Future configurable delivery targets require a first-class LPE model, not a
  MAPI-only table.

Until that model exists, `RopSetReceiveFolder` must stay limited to confirming
the canonical map. It must not:

- create a MAPI-local receive-folder table;
- change Internet mail delivery routing;
- redirect calendar/contact object creation into arbitrary mail folders;
- override mailbox role folders or special-folder identity;
- persist Outlook client cache state as mailbox truth.

## Canonical Model Needed For Wider Mutation

Wider receive-folder mutation would need an explicit canonical service and
storage contract for:

- per-account message-class routing;
- allowed target folder kinds and tenant isolation;
- delivery-time interaction with canonical submission, inbound delivery, and
  collaboration item creation;
- change-log events so MAPI, EWS, JMAP, IMAP where applicable, and administrative
  APIs converge on the same state;
- migration semantics for existing mailboxes.

No schema change should be added just to satisfy a MAPI write unless real
Outlook evidence shows the bounded acknowledgement is insufficient.

## Tests And Evidence

Existing evidence covers the bounded behavior:

- `mapi_over_http_execute_returns_receive_folder_and_store_state`
- `mapi_over_http_get_receive_folder_uses_message_class_matching`
- `mapi_over_http_get_receive_folder_table_requires_private_logon_handle`
- `mapi_over_http_get_receive_folder_requires_private_logon_handle`
- `mapi_over_http_set_receive_folder_requires_private_logon_handle`
- `mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar`
- `mapi_over_http_get_receive_folder_calendar_fid_opens_default_calendar_with_custom_only_collections`

Before widening behavior, add tests proving:

- invalid folder targets do not mutate routing;
- canonical message delivery still lands in the expected Inbox/Sent paths;
- calendar and contact item creation still use canonical collaboration storage;
- changed routing, if ever supported, is visible through all relevant protocols
  and survives restart without MAPI session state;
- real Outlook 2016 and Outlook 2019 traces require the wider mutation.

## Public MAPI Autodiscover Impact

The fixed receive-folder map and bounded acknowledgement are part of the profile
and Calendar bootstrap surface. Missing or broken `RopGetReceiveFolder` /
`RopGetReceiveFolderTable` behavior can block public MAPI autodiscover.

Arbitrary `RopSetReceiveFolder` mutation does not block public MAPI
autodiscover unless real Outlook profile or cached-mode evidence shows Outlook
requires it for supported versions.
