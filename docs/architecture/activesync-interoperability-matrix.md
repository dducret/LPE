# ActiveSync Interoperability Matrix

## Current State/Functionality Overview

The matrix defines the ActiveSync client scenarios required for the supported adapter surface. The scope stays limited to `docs/architecture/activesync-mvp.md`.

## Implementation/Usage

- Test both `Basic` and bearer authentication where the client supports them.
- Verify `Provision`, `FolderSync`, mailbox `Sync`, `SendMail`, `SmartReply`, `SmartForward`, `Ping`, `Search`, and `ItemOperations Fetch`.
- Treat canonical `Sent` visibility after send as mandatory.
- Treat sync-key stability, retry behavior, and paged continuation as mandatory.
- Treat attachment fetch consistency across send, sync, and fetch as mandatory.
- Keep Outlook desktop ActiveSync testing separate from classic Outlook Exchange-account testing.
- Do not use ActiveSync as the Outlook for Windows desktop Exchange-account gate; that belongs to the EWS/MAPI readiness path.

## Reference Table/List

| Code | Scenario | Area |
| --- | --- | --- |
| `S1` | account enrollment with `Provision` and first `FolderSync` | auth, device policy, folder discovery |
| `S2` | mailbox `Sync` with `SyncKey = 0`, priming, and first page | initial sync |
| `S3` | repeated `Sync` with no changes | idempotence |
| `S4` | delegated mailbox folder discovery and sync | shared mail |
| `S5` | `SendMail` creates canonical `Sent` copy | submission |
| `S6` | `SmartReply` and `SmartForward` create canonical `Sent` copy | submission |
| `S7` | `Ping` detects folder changes | long poll |
| `S8` | attachment `Fetch` returns canonical blobs | attachments |
| `S9` | contact create/update/delete through `Sync` | contacts |
| `S10` | calendar create/update/delete through `Sync` | calendar |
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
| stable repeated sync | `stable_sync_does_not_reload_full_email_payloads_without_changes` |
| sync-key recovery | `stale_sync_key_is_rejected_after_a_completed_round` |
| mobile send | `send_mail_uses_canonical_submission_model` |
| smart reply | `smart_reply_uses_source_recipients_and_canonical_submission` |
| smart forward | `smart_forward_reuses_source_message_and_attachments` |
| attachment fetch | `item_operations_fetch_returns_attachment_bytes` |
| search | `search_queries_canonical_mail_projection` |
| long poll | `ping_reports_changed_collections_after_sync_state_exists` |
| long-poll reconnect | `ping_reconnects_after_service_restart_using_persisted_sync_state` |
| contact/calendar mutations | `sync_contact_and_calendar_mutations_update_canonical_models` |
