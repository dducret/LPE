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
| Classic Outlook desktop as direct ActiveSync client | `Basic`, bearer where available | ActiveSync `16.1` | `S1` `S2` `S3` `S4` `S5` `S6` `S7` `S8` `S11` `S12` |
