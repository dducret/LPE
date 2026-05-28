# ActiveSync Interoperability Matrix

## Current State/Functionality Overview

The matrix defines the ActiveSync client scenarios required for the supported adapter surface. The scope stays limited to `docs/architecture/activesync-mvp.md`.

## Implementation/Usage

- Test both `Basic` and bearer authentication where the client supports them.
- Verify `Provision`, `FolderSync`, mailbox `Sync`, `SendMail`, `SmartReply`, `SmartForward`, `Ping`, `Search`, and `ItemOperations Fetch`.
- Treat canonical `Sent` visibility after send as mandatory.
- Treat canonical Outlook follow-up mail state as mailbox `Sync` behavior: flagged or completed messages include the ActiveSync `email:Flag` container with documented Email and Tasks child elements; client `Change` commands can set, complete, or clear the same canonical follow-up state.
- Treat sync-key stability, retry behavior, and paged continuation as mandatory.
- Treat attachment fetch consistency across send, sync, and fetch as mandatory.
- Keep Outlook desktop ActiveSync testing separate from classic Outlook Exchange-account testing.
- Do not use ActiveSync as the Outlook for Windows desktop Exchange-account gate; that belongs to the EWS/MAPI readiness path.
- Distinguish local harness evidence from real-client evidence. `cargo test -p lpe-activesync` and the preflight helper below prove local behavior and publication shape only; Outlook mobile and iOS Mail passes require device logs/screenshots or packet/server traces from those clients.

Run the live preflight before manual client enrollment:

```powershell
python tools/activesync_mobile_lab_preflight.py --base-url https://mail.example.test --email alice@example.test --password <mailbox-password>
```

Use `--insecure` only for a closed lab with a temporary certificate. The helper checks:

- `OPTIONS /Microsoft-Server-ActiveSync` advertises exactly protocol version
  `16.1` and only the implemented command set.
- anonymous `OPTIONS` still returns ActiveSync capability headers with a `Basic` challenge when no password is supplied.
- Autodiscover v2 `Protocol=ActiveSync` and `Protocol=MobileSync` return the ActiveSync endpoint.
- default Outlook POX Autodiscover does not publish `MobileSync`.
- MobileSync POX Autodiscover publishes the ActiveSync endpoint for mobile-client schema requests.

The helper does not exercise `Provision`, `FolderSync`, `Sync`, send flows, attachments, search, `Ping`, reconnect, stale-key recovery, or `Sent` visibility. Those remain real-client lab steps.

## Microsoft Source Log

| Page title | URL | Topic or claim supported | Date accessed | Uncertainty or ambiguity |
| --- | --- | --- | --- | --- |
| `[MS-ASEMAIL]: Updating E-Mail Flags` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-asemail/56dc3bae-2bc9-4a5c-be69-ac5e4ee8f90c | ActiveSync email flag actions use `Status = 2` for flagged, `Status = 1` plus completion times for complete, and `Status = 0` or an empty flag for clear. | 2026-05-20 | The page directly defines update behavior; using the same shape for read projection is an engineering inference aligned with Sync payload symmetry. |
| `[MS-ASEMAIL]: FlagType` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-asemail/157c6b73-9401-44e9-b9e0-fbeadef817f6 | `FlagType` is an optional Email namespace child of `Flag`, and common values include `Flag for follow up`. | 2026-05-20 | The value is customizable; LPE emits the common Outlook-compatible value until canonical per-message ActiveSync flag labels are introduced. |
| `[MS-ASEMAIL]: DueDate` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-asemail/cc1cc467-4ecd-4ef6-8ee8-d0e52e9da8b3 | `tasks:DueDate` is an optional `Flag` child; when setting a flag, start/due and UTC start/due must be supplied together or all be null. | 2026-05-20 | This is a write-side validation rule; LPE applies the same all-or-none pairing when projecting stored follow-up dates. |
| `[MS-ASEMAIL]: Sync Command Request` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-asemail/a8602ea5-a3f3-4426-83b5-a8d5315a953d | `Flag` is an E-mail class element carried under `airsync:ApplicationData` for Sync changes. | 2026-05-20 | The page is request-focused; server response use follows ActiveSync ApplicationData symmetry and local client interop tests remain required. |
| `[MS-ASCMD]: Change` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-ascmd/3e2b243a-d052-407f-bfc0-ee0de82e1e01 | A Sync `Change` can carry `ApplicationData`, and flag-only changes leave other in-schema properties untouched. | 2026-05-20 | LPE has not enabled ActiveSync flag writes yet; this supports the future mutation boundary. |
| `[MS-ASWBXML]: Code Page 2: Email` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-aswbxml/06f4ff28-ac7b-4c56-a9e2-6eb33dc55c83 | Email WBXML tokens for `Flag`, `Status`, `FlagType`, and `CompleteTime`. | 2026-05-20 | Learn showed an authorization banner but the token table was visible. |
| `[MS-ASWBXML]: Relationship to Protocols and Other Algorithms` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-aswbxml/2e57ccd4-cf38-45a0-bb11-95304bd1180d | ActiveSync WBXML code page 9 is the Tasks namespace used by `[MS-ASTASK]`. | 2026-05-20 | The current Learn search did not surface a dedicated Code Page 9 table page; token values below were cross-checked against Microsoft’s Exchange ActiveSync client implementation guidance. |
| `Implementing an Exchange ActiveSync client: the transport mechanism` | https://learn.microsoft.com/en-us/previous-versions/office/developer/exchange-server-interoperability-guidance/hh361570(v=exchg.140) | Microsoft sample code lists Tasks code page 9 tokens used here: `DateCompleted`, `DueDate`, `UtcDueDate`, `StartDate`, and `UtcStartDate`. | 2026-05-20 | This is older Exchange Server 2010 implementation guidance rather than the current Open Specifications page; it is used only because the current code-page-9 table was not discoverable on Learn during this slice. |

## Real-Client Lab Gate

Record one evidence row per client/build/device. Do not mark the gate passed from local tests alone.

| Field | Required value |
| --- | --- |
| Date | calendar date of the run |
| Public host | exact `LPE-CT` HTTPS host used by the client |
| Server revision | Git commit or release build |
| Client | Outlook mobile for iOS, Outlook mobile for Android, or iOS Mail |
| Client build | app version and OS version |
| Account | test mailbox, shared mailbox if used, and auth method |
| Preflight | command line and pass/fail output from `tools/activesync_mobile_lab_preflight.py` |
| Evidence | server trace ids, sanitized HTTP/WBXML transcript ids, client screenshots, or device logs |
| Result | pass/fail plus defects opened or fixed |

### Outlook Mobile Checklist

Run this checklist for Outlook mobile on iOS and Android when both are available. Publication requires real Outlook mobile and iOS Mail evidence; Android evidence is additional coverage unless the release explicitly targets it.

| Step | Expected evidence |
| --- | --- |
| Enrollment | Account adds successfully through mobile Autodiscover or explicit Exchange/ActiveSync server entry; server logs show the mobile ActiveSync endpoint, not Outlook desktop MAPI/EWS publication. |
| `OPTIONS` and version negotiation | Client probes `OPTIONS /Microsoft-Server-ActiveSync`; response includes exactly `MS-ASProtocolVersions: 16.1` and the implemented command list. The client uses `MS-ASProtocolVersion: 16.1` on later POSTs; explicit unsupported versions receive a predictable `400 Bad Request` with the supported version set. |
| `Provision` | Initial Provision and acknowledgment complete with policy status `1`; no unsupported device-policy requirement blocks enrollment. |
| `FolderSync` | Initial `FolderSync` with key `0` returns canonical folders including `Inbox`, `Sent`, `Drafts`, `Trash`, and user mail folders with stable server ids. |
| Initial `Sync` | Inbox initial collection `Sync` starts with key `0`, receives a new key, and retrieves the first page without duplicate or missing messages. |
| Follow-up flags | Flagged and completed messages expose `email:Flag` with `Status`, `FlagType`, paired Tasks start/due UTC dates when present, and completion times when complete; client set/complete/clear operations update the same state visible through JMAP, IMAP, and MAPI projections. |
| Incremental `Sync` | A message delivered after initial sync appears once; a no-change sync does not reload the mailbox. |
| `SendMail` | Message sent from Outlook mobile is accepted through ActiveSync, relayed through canonical submission, and appears in LPE `Sent`. |
| `SmartReply` | Reply from a synced message sends successfully and creates the authoritative `Sent` copy. |
| `SmartForward` | Forward from a synced message sends successfully, includes expected original content/attachments, and creates the authoritative `Sent` copy. |
| Attachments | Client can open a received attachment through `ItemOperations Fetch`; forwarded attachment content remains readable by the recipient. |
| `Search` | Mail search returns expected mailbox results; selecting a result opens/fetches the message instead of producing a client error. |
| `Ping` no-change | After a completed collection `Sync`, `Ping` returns no-change status when no monitored folder changes occur during the heartbeat. |
| `Ping` change | New mail in a monitored folder wakes `Ping`, returns the changed folder id, and the following `Sync` retrieves the message. |
| Reconnect | Restart the service or drop the mobile network after a valid `Ping`; the client reconnects without deleting and recreating the account. |
| Stale-key recovery | Present or simulate a stale collection key; client performs the required recovery flow and resumes sync after `FolderSync` or collection re-prime. |
| `Sent` visibility | Every `SendMail`, `SmartReply`, and `SmartForward` message is visible in LPE web/JMAP/IMAP `Sent` and on the mobile client after sync. |

### iOS Mail Checklist

Run this checklist with the native iOS Mail account type that uses Exchange ActiveSync.

| Step | Expected evidence |
| --- | --- |
| Enrollment | Account adds successfully through mobile Autodiscover or explicit server entry; Mail, Contacts, and Calendars toggles may be enabled only for implemented classes. |
| `OPTIONS` and version negotiation | Device probes `OPTIONS /Microsoft-Server-ActiveSync`; response includes exactly `16.1` and the implemented command list; later POSTs use `16.1`, while explicit unsupported versions receive a predictable `400 Bad Request` with the supported version set. |
| `Provision` | Device policy handshake completes; iOS does not demand an unsupported policy before first sync. |
| `FolderSync` | Initial folder hierarchy includes canonical mail folders, contacts, and calendar collections that the user enabled. |
| Initial `Sync` | Mail, contacts, and calendar collections that are enabled start with collection sync key `0` and receive stable follow-up keys. |
| Follow-up flags | Flagged and completed messages expose ActiveSync `email:Flag` state, and client set/complete/clear operations update canonical state visible through JMAP, IMAP, and MAPI projections. |
| Incremental `Sync` | New mail, contact changes, and calendar changes appear once and preserve canonical fields documented in `activesync-mvp.md`. |
| `SendMail` | Sending from iOS Mail uses ActiveSync submission and creates the authoritative LPE `Sent` copy. |
| Attachments | Received attachment opens through `ItemOperations Fetch`; sent attachment passes canonical validation and is visible to the recipient. |
| `Search` | Native Mail search returns expected mailbox results and can open the selected message. |
| `Ping` no-change | Empty/no-change long poll succeeds after prior collection sync. |
| `Ping` change | New mail wakes the monitored folder and the following sync retrieves the message. |
| Reconnect | Airplane-mode toggle, network switch, or service restart does not force account recreation. |
| Stale-key recovery | Stale key response leads iOS Mail to re-prime folder or collection state and resume sync. |
| `Sent` visibility | Sent messages are visible in LPE web/JMAP/IMAP `Sent` and on iOS Mail after sync. |

Known unsupported ActiveSync behavior for this gate remains the unsupported set in `docs/architecture/activesync-mvp.md`: full Exchange server semantics, client `SMTP`, ActiveSync task class, legacy `GetAttachment`, multipart `ItemOperations` responses, non-draft mail edits through `Sync`, unsupported contact fields, and unsupported calendar fields such as recurrence exceptions and all-day events.

## Reference Table/List

| Code | Scenario | Area |
| --- | --- | --- |
| `S1` | account enrollment with `Provision` and first `FolderSync` | auth, device policy, folder discovery |
| `S2` | mailbox `Sync` with `SyncKey = 0`, priming, and first page | initial sync |
| `S3` | repeated `Sync` with no changes | idempotence |
| `S3A` | mailbox `Sync` projects and mutates canonical Outlook follow-up mail state as ActiveSync `email:Flag` | mail flags |
| `S4` | delegated mailbox folder discovery and sync | shared mail |
| `S5` | `SendMail` creates canonical `Sent` copy | submission |
| `S6` | `SmartReply` and `SmartForward` create canonical `Sent` copy | submission |
| `S7` | `Ping` detects folder changes | long poll |
| `S8` | attachment `Fetch` returns canonical blobs | attachments |
| `S9` | contact create/update/delete through `Sync` for canonical name, email, phone, organization, title, and notes | contacts |
| `S10` | calendar create/update/delete through `Sync` for canonical UID, title, start, duration, time-zone string, location, body, attendees, and simple recurrence | calendar |
| `S11` | folder rename/delete constraints | mailbox safety |
| `S12` | reconnect after stale or invalid sync state | recovery |

| Client | Auth | Protocol target | Required scenarios |
| --- | --- | --- | --- |
| Outlook mobile for iOS | bearer, `Basic` fallback | ActiveSync `16.1` | `S1` `S2` `S3` `S5` `S6` `S7` `S8` |
| Outlook mobile for Android | bearer, `Basic` fallback | ActiveSync `16.1` | `S1` `S2` `S3` `S5` `S6` `S7` `S8` |
| iOS Mail | `Basic`, bearer where available | ActiveSync `16.1` | `S1` `S2` `S3` `S5` `S7` `S8` `S9` `S10` |
| Apple Mail on macOS when configured as Exchange ActiveSync | `Basic`, bearer where available | ActiveSync `16.1` | `S1` `S2` `S3` `S5` `S7` `S8` `S9` `S10` `S12` |

| Lab checkpoint | Local coverage |
| --- | --- |
| enrollment and folder discovery | `folder_sync_returns_mail_and_collaboration_collections` |
| initial and paged sync | `sync_key_zero_primes_then_returns_paged_more_available_changes` |
| follow-up flag read/write projection | `sync_projects_email_followup_flag_state`; `sync_change_updates_followup_flag_state` |
| stable repeated sync | `stable_sync_does_not_reload_full_email_payloads_without_changes` |
| sync-key recovery | `stale_sync_key_is_rejected_after_a_completed_round` |
| mobile send | `send_mail_uses_canonical_submission_model` |
| smart reply | `smart_reply_uses_source_recipients_and_canonical_submission` |
| smart forward | `smart_forward_reuses_source_message_and_attachments` |
| attachment fetch | `item_operations_fetch_returns_attachment_bytes` |
| search | `search_queries_canonical_mail_projection` |
| long poll | `ping_no_changes_returns_no_change_status`; `ping_reports_changed_folder_ids_as_folder_values`; `ping_detects_changes_across_multiple_monitored_collections` |
| long-poll validation | `ping_empty_request_without_cached_parameters_returns_missing_parameters`; `ping_invalid_folder_id_requires_folder_sync`; `ping_rejects_unsynchronized_folders`; `ping_heartbeat_outside_supported_range_returns_limit`; `ping_too_many_monitored_folders_returns_max_folders`; `ping_surfaces_hierarchy_change_as_folder_sync_required` |
| long-poll reconnect | `ping_reconnects_after_service_restart_using_persisted_sync_state` |
| contact/calendar mutations | `sync_contact_and_calendar_mutations_update_canonical_models`; `sync_contact_create_update_delete_round_trips_canonical_fields`; `sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees` |
| contact/calendar projection and no-change sync | `sync_contact_and_calendar_projection_includes_supported_application_data` |
