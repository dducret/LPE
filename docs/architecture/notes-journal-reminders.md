# Notes, Journal, and Reminders

## Current State/Functionality Overview

`LPE` stores Outlook-compatible Notes and Journal entries as canonical
tenant/account-owned user-visible state. Reminders are not stored as ordinary
mailboxes or protocol-local MAPI folders; they are computed from canonical
objects that carry reminder metadata.

## Microsoft Source Log

| Page title | URL | Supported claim | Accessed | Ambiguity |
| --- | --- | --- | --- | --- |
| `[MS-OXOSFLD]: Special Folders Protocol` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxosfld/a60e9c16-2ba8-424b-b60c-385a8a2837cb | Exchange special folders provide stable default folder identity for object classes and non-user-visible application data. | 2026-05-19 | The page is an entry point; item schema comes from object-specific specs. |
| `[MS-OXOSFLD]: List of Special Folders` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxosfld/68a85898-84fe-43c4-b166-4711c13cdd61 | Journal, Notes, Tasks, and Reminders are special folders; Tasks uses `IPF.Task`, Reminders uses `Outlook.Reminder`. | 2026-05-19 | It identifies folder classes, not the complete canonical schema for a non-Exchange store. |
| `[MS-OXOSFLD]: Search Criteria for Search Special Folders` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxosfld/1169ebe3-22c8-4b77-bc58-791e8a91973f | Reminders is a search folder over eligible reminder-bearing objects and excludes Deleted Items, Junk, Drafts, Outbox, Sync Issues, and failure folders. | 2026-05-19 | `LPE` does not model calendar/tasks as mail folders, so folder exclusion maps to canonical object state and deletion visibility. |
| `[MS-OXONOTE]: Note Object Protocol` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxonote/6bf4ed7e-316c-4a3c-be27-5ec93e7ab39f | Notes represent brief sticky-note objects. | 2026-05-19 | Several detailed property pages are access-limited; `title`, `body_text`, `color`, and categories are an engineering inference for Outlook projection readiness. |
| `[MS-OXOJRNL]: Journal Object Protocol` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxojrnl/2aa04fd2-0f36-4ce4-9178-c0fc70aa8d43 | Journal objects track activity related to meetings, tasks, contacts, or application files. | 2026-05-19 | The overview does not require every MAPI named property to become first-class canonical storage. |
| `[MS-OXOJRNL]: Journal Object for a Telephone Call Example` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxojrnl/f4f1636e-2571-4994-bb8e-d2c801ecc901 | Journal entries commonly carry start/end, log type, companies, contacts, and body/notes data. | 2026-05-19 | The example is informative, so `entry_type`, companies, and contacts are stored as projection-ready metadata rather than a full MAPI property bag. |
| `[MS-OXORMDR]: Reminder Settings Protocol` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxormdr/5454ebcc-e5d1-4da8-a598-d393b101caab | Reminder behavior is defined through reminder-related properties on objects. | 2026-05-19 | Some behavior sections are access-limited; the canonical model stores only reminder set/time/dismissal facts needed for query behavior. |
| `[MS-OXORMDR]: Glossary` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxormdr/117aefd1-2f9a-4378-b32c-bd8397f2cf70 | The full reminder domain excludes Deleted Items, Junk Email, Drafts, Outbox, Conflicts, Local Failures, Server Failures, and Sync Issues. | 2026-05-19 | `LPE` maps this to canonical object visibility/status because tasks/events are not mail-folder rows. |
| `[MS-OXOTASK]: PidLidTaskResetReminder Property` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxotask/e3124e37-9872-4a64-8467-ea468e74837e | Recurring task reminder behavior needs reset/dismissal metadata beyond a single reminder-set flag. | 2026-05-19 | Recurrence-specific reminder expansion is deferred; `reminder_reset` is stored for projection compatibility. |
| `[MS-OXOCAL]: Calendar Object` | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxocal/fa22144a-fc6a-48d8-ad75-a7a5e21267bf | Calendar objects can carry reminder-related properties from MS-OXORMDR. | 2026-05-19 | Recurring-instance reminder expansion remains future interoperability work. |

## Canonical Storage

- `notes` stores account-owned sticky-note data: title/display text, body text,
  Outlook-compatible color metadata, categories, source metadata, and timestamps.
- `journal_entries` stores account-owned journal data: subject, body, entry type,
  MAPI message class projection metadata, start/end/occurred timestamps,
  companies, contacts, source metadata, and timestamps.
- `calendar_events` and `tasks` carry `reminder_set`, `reminder_at`, and
  `reminder_dismissed_at`. `tasks` also carries `reminder_reset` for future
  recurring-task reminder projection.
- There is no `reminders` table. Reminder APIs query canonical tasks and
  calendar events.

Notes and Journal do not currently carry reminder metadata. Including them in
Reminder results would be an engineering inference from generic reminder-bearing
Message object language, while the current `LPE` API foundation can satisfy the
required Outlook-compatible behavior through calendar and task objects first.

## API Surface

The client API uses authenticated mailbox account context:

- `GET /api/mail/notes`
- `POST /api/mail/notes`
- `GET /api/mail/notes/{note_id}`
- `DELETE /api/mail/notes/{note_id}`
- `GET /api/mail/journal`
- `POST /api/mail/journal`
- `GET /api/mail/journal/{entry_id}`
- `DELETE /api/mail/journal/{entry_id}`
- `GET /api/mail/reminders?includeInactive=true`

The Reminders endpoint returns computed rows with `sourceType`, `sourceId`,
`title`, `dueAt`, `reminderAt`, `dismissedAt`, `completedAt`, and `status`.
Default queries return active due/pending reminders. `includeInactive=true`
also returns dismissed, completed, and explicitly excluded rows for diagnostics
and compatibility tests.

## JMAP Foundation

`LPE` exposes a private JMAP extension capability,
`https://l-p-e.ch/jmap/outlook`, for Outlook-compatible collaboration objects
that do not fit standard JMAP Mail object types. The first foundation methods
are:

- `Note/get`
- `Note/query`
- `Note/changes`
- `Note/queryChanges`
- `Note/set`
- `JournalEntry/get`
- `JournalEntry/query`
- `JournalEntry/changes`
- `JournalEntry/queryChanges`
- `JournalEntry/set`
- `Reminder/query`

These methods are private LPE extensions. They must not be advertised as
standard JMAP Mail support, and they must not represent Notes or Journal entries
as JMAP `Mailbox` or `Email` objects. Reminder results remain computed from
canonical reminder-bearing objects.

Notes and Journal writes allocate account/category modseqs in `account_sync_state`,
append object-level `mail_change_log` rows, and write tombstones for deletes.
This enables private `changes` and `queryChanges` support without introducing
Exchange-local state.

`Note` and `JournalEntry` are also valid private WebSocket push data types.
Push state is recomputed from the canonical storage projection when the `notes`
or `journal` canonical change category advances.

## Deferred Work

- MAPI ROP behavior for Notes, Journal, and Reminders remains deferred to the
  MAPI over HTTP adapter phase.
- Web UI is intentionally not part of this foundation change.
