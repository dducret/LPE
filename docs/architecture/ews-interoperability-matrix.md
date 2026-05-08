# EWS Interoperability Matrix

## Current State/Functionality Overview

The EWS matrix defines the live checks required for the supported `lpe-exchange` EWS surface. It validates canonical mailbox, contacts, calendar, task, and submission behavior through `/EWS/Exchange.asmx`.

## Implementation/Usage

- Use the live smoke harness against the public or local EWS URL.
- Require authentication.
- Require canonical `Sent` visibility after EWS send.
- Require contact and calendar create-read-delete checks.
- Require task checks where EWS task operations are published.
- Keep `MAPI/EMSMDB`, `MAPI/NSPI`, and `/rpc/rpcproxy.dll` checks in the Outlook/MAPI readiness path.
- Unsupported Exchange operations must return parseable EWS errors, not generic transport failures.

## Reference Table/List

| Check | Requirement |
| --- | --- |
| EWS endpoint | `https://mail.example.test/EWS/Exchange.asmx` |
| Smoke script | `tools/ews_live_smoke_check.py` |
| RCA script | `tools/rca_outlook_connectivity_check.py` |
| Mail | find, get, create/send, delete where supported |
| Contacts | create, read, update, delete |
| Calendar | create, read, update, delete, busy status |
| Tasks | create, read, update, delete where supported |
| Submission | canonical `Sent` copy visible after send |

| Operation | 0.2.0 status |
| --- | --- |
| `SyncFolderHierarchy` | supported for canonical folder hierarchy |
| `FindFolder` / `GetFolder` | supported for canonical folder discovery |
| `FindItem` / `GetItem` | supported for canonical mail, contacts, calendar, and tasks where exposed |
| `SyncFolderItems` | supported for canonical mail, contacts, calendar, and tasks |
| `CreateItem` | supported for mail drafts/send, contacts, calendar events, and tasks within bounded mappings |
| `UpdateItem` | supported for canonical message read/flag state and collaboration item updates within bounded mappings |
| `DeleteItem` | supported for bounded canonical mail/contact/calendar/task deletion |
| `MoveItem` / `CopyItem` | supported for bounded canonical mail movement/copy behavior |
| `CreateFolder` / `DeleteFolder` | supported for custom canonical mail folders; protected folders are guarded |
| `GetAttachment` / `CreateAttachment` / `DeleteAttachment` | supported for bounded canonical attachment flows |
| `ResolveNames` | supported against canonical account/contact visibility |
| `GetUserAvailability` | supported for canonical calendar availability |
| `GetServerTimeZones` | supported static compatibility response |
| `GetUserOofSettings` / `SetUserOofSettings` | supported through canonical vacation/Sieve behavior |
| `Subscribe` / `GetEvents` / `Unsubscribe` | supported for bounded pull-subscription compatibility; `Subscribe` accepts pull subscriptions, mailbox `CreateItem` and `DeleteItem` calls are recorded in a short-lived in-process pull queue, `GetEvents` returns queued `CreatedEvent`, `NewMailEvent`, and `DeletedEvent` notifications before falling back to visible-message projections, emits mailbox-scoped compatibility `CreatedEvent` probes when no durable event queue is available, or status notifications for non-mailbox subscriptions, and push/streaming notifications remain out of scope |
| `GetRoomLists`, `FindPeople`, `ExpandDL`, `GetDelegate`, `GetUserConfiguration`, `GetSharingMetadata`, `GetSharingFolder` | intentionally unsupported in `0.2.0`; must return EWS error responses |
