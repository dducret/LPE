# JMAP Contacts and Calendars

## Current State/Functionality Overview

`lpe-jmap` exposes contacts and calendars over canonical `contacts` and `calendar_events`. It uses mailbox authentication and does not introduce JMAP-specific collaboration storage.

## Implementation/Usage

- Endpoints:
  - `GET /api/jmap/session`
  - `POST /api/jmap/api`
- Authentication:
  - mailbox login through `/api/mail/auth/login`
  - JMAP requests scoped to the authenticated account
- Session capabilities:
  - contacts capability
  - calendars capability
  - core JMAP capability
  - private LPE Outlook compatibility capability: `https://l-p-e.ch/jmap/outlook`
- Supported methods:
  - `AddressBook/get`
  - `AddressBook/query`
  - `AddressBook/changes`
  - `AddressBook/queryChanges`
  - `AddressBook/set`
  - `AddressBook/import`
  - `AddressBook/copy`
  - `ContactCard/get`
  - `ContactCard/set`
  - `ContactCard/changes`
  - `ContactCard/query`
  - `ContactCard/queryChanges`
  - `ContactCard/import`
  - `ContactCard/copy`
  - `Calendar/get`
  - `Calendar/query`
  - `Calendar/changes`
  - `Calendar/queryChanges`
  - `CalendarEvent/get`
  - `CalendarEvent/set`
  - `CalendarEvent/changes`
  - `CalendarEvent/query`
  - `CalendarEvent/queryChanges`
  - `CalendarEvent/import`
  - `CalendarEvent/copy`
  - `Note/get`
  - `Note/query`
  - `Note/changes`
  - `Note/queryChanges`
  - `Note/set`
  - `Note/import`
  - `Note/copy`
  - `JournalEntry/get`
  - `JournalEntry/query`
  - `JournalEntry/changes`
  - `JournalEntry/queryChanges`
  - `JournalEntry/set`
  - `JournalEntry/import`
  - `JournalEntry/copy`
  - `Share/get`
  - `Share/query`
  - `Share/changes`
  - `Share/queryChanges`
  - `Share/set`
  - `Share/import`
  - `Share/copy`
  - `DurableChange/get`
  - `DurableChange/query`
  - `DurableChange/changes`
  - `DurableChange/queryChanges`
  - `DurableChange/set`
  - `DurableChange/import`
  - `DurableChange/copy`
  - `Reminder/query`
  - `Reminder/get`
  - `Reminder/changes`
  - `Reminder/queryChanges`
  - `Reminder/set`
  - `Reminder/import`
  - `Reminder/copy`
- Mapping:
  - one canonical `default` address book per account
  - one canonical `default` calendar per account
  - contacts map to canonical `contacts`
  - events map to canonical `calendar_events`
  - private `Note` maps to canonical `notes`
  - private `JournalEntry` maps to canonical `journal_entries`
  - private `Reminder` is computed from reminder-bearing canonical tasks and calendar events
  - private `Share` projects and mutates canonical mailbox, sender, contact, calendar, and task sharing grants
  - private `DurableChange` exposes canonical change cursor metadata and categories for clients that need durable sync diagnostics
- Payloads:
  - `CalendarEvent` participant metadata is stored in `calendar_events.attendees_json` as an object containing organizer and attendee fields; older array-only attendee payloads are migrated into the object form.
  - `CalendarEvent/get`, `CalendarEvent/set`, `CalendarEvent/query`, and `CalendarEvent/changes` are limited to canonical `calendar_events` fields already owned by LPE: `id`, `uid`, `@type`, `title`, `start`, `duration`, `timeZone`, `allDay`, `status`, `sequence`, `recurrenceRule`, opaque `recurrence`, opaque `recurrenceOverrides`, `locations` by name, `organizer`, `participants`, `description`, `descriptionContentType`, `bodyHtml`, and `calendarIds`. This is not a full JSCalendar implementation; unsupported fields are rejected rather than stored as protocol-local extensions.
  - `Share` returns a stable object-specific projection with `id`, `@type: "Share"`, `type`, `grantId`, owner and grantee account metadata, `rights`, and `created`/`updated` timestamps. Sender shares include `senderRight`; task-list shares include `taskListId` and `taskListName`.
  - `DurableChange` returns the singleton `canonical` object with `@type: "DurableChange"`, `scope: "account"`, `cursor`, `isAppendOnly: true`, `mayRead: true`, `mayWrite: false`, and category objects listing affected JMAP object families.
- Push:
  - private `Note` and `JournalEntry` are WebSocket push data types
  - their state changes are driven by canonical `notes` and `journal`
    categories, not protocol-local sync state
- Rules:
  - rights are bounded by authenticated account and canonical grants
  - no JMAP-only collection store
  - no protocol-local sharing model
  - `Share` and `DurableChange` are canonical private JMAP projections, not MAPI session or subsystem objects
  - `DurableChange` is an append-only sync cursor projection; write methods remain read-only error surfaces and must not mutate canonical history
  - cursor-backed `changes` responses replay durable object log rows; contact, calendar-event, and task surfaces expand collection and grant dependency rows to affected child object ids instead of falling back to full state diffs
  - `Share/changes` and `Reminder/changes` use string-id durable replay because their JMAP ids are typed projections such as `mailbox:<grantId>`, `sender:<grantId>`, `task:<taskId>`, `calendar:<eventId>`, and `mail:<messageId>`
  - `ContactCard/import`, `ContactCard/copy`, `CalendarEvent/import`, `CalendarEvent/copy`, `Note/import`, `Note/copy`, `JournalEntry/import`, and `JournalEntry/copy` are canonical create-style writes over the same payloads accepted by each object's `set` create branch
  - generic private canonical `query` and `queryChanges` handlers persist query snapshots through the JMAP query-state store when available, so cursor-backed clients do not depend on large embedded query-state id lists
  - generic private canonical `get` handlers honor JMAP `ids`, `notFound`, and `properties` projection semantics while always returning object `id`
  - unsupported canonical write surfaces return operation-specific `notCreated`, `notUpdated`, and `notDestroyed` set errors instead of top-level method errors
  - generic private canonical `query` responses use deterministic id ordering for stable client pagination
  - query/change behavior uses canonical timestamps and state
  - private Outlook compatibility methods must not overload JMAP Mail `Mailbox` or `Email`
  - `Calendar/set`, `Calendar/import`, and `Calendar/copy` are intentionally not advertised or supported until canonical non-default calendar collection lifecycle semantics are implemented; they return `unknownMethod`, and event writes use `CalendarEvent/*` against the canonical calendar tables

## Reference Table/List

| JMAP object | Canonical source |
| --- | --- |
| `AddressBook` | canonical default address book |
| `ContactCard` | `contacts` |
| `Calendar` | canonical default calendar |
| `CalendarEvent` | `calendar_events` |
| private `Note` | `notes` |
| private `JournalEntry` | `journal_entries` |
| private `Reminder` | computed from `tasks` and `calendar_events` |
