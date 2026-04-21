# DAV MVP

## Objective

This document describes the current `CardDAV`, `CalDAV`, and first `VTODO` interoperability adapter implemented in `LPE`.

The `crates/lpe-dav` crate exposes a deliberately small but more interoperable DAV compatibility layer for contacts, calendar data, and canonical tasks without introducing a DAV-specific storage model. It remains an adapter over the canonical collaboration data already owned by `LPE`.

## Architectural principles

- `JMAP` remains the primary modern protocol axis
- `CardDAV` and `CalDAV` are compatibility adapters only
- mailbox-account authentication is reused rather than duplicated
- `contacts` remains the source of truth for address-book data
- `calendar_events` remains the source of truth for calendar data, including the minimal interoperability metadata now needed for DAV clients
- `tasks` remains the source of truth for task data
- the adapter must not introduce a separate storage, tenant, or rights model
- each DAV request is scoped to the authenticated principal and the canonical same-tenant grants resolved by `LPE`

## Endpoints

- `/.well-known/carddav`
- `/.well-known/caldav`
- `/dav/`
- `/dav/principals/me/`
- `/dav/addressbooks/me/{collection-id}/`
- `/dav/calendars/me/{collection-id}/`

Without the Debian reverse proxy, these routes are exposed directly by the Rust service.

With the documented `/api/` reverse proxy, they are reachable through `/api/.well-known/carddav`, `/api/.well-known/caldav`, and `/api/dav/...`.

## Authentication

- mailbox-account `Bearer` sessions created by `/api/mail/auth/login` are accepted
- mailbox `OAuth2` bearer access tokens created by `/api/mail/auth/oauth/access-token` are also accepted when the token scope includes `dav`
- mailbox-account `Basic` authentication is also accepted for DAV-compatible clients
- there is no separate DAV account model outside the normal `LPE` mailbox account

## Supported methods

The MVP supports:

- `OPTIONS`
- `PROPFIND`
- `REPORT`
- `GET`
- `PUT`
- `DELETE`

The implementation keeps discovery and synchronization intentionally minimal:

- `PROPFIND` exposes the root, current principal, every accessible address-book collection, every accessible calendar collection, every accessible task collection, and collection members
- `REPORT` supports collection reads, multiget-style `href` targeting, simple text-match filtering, minimal calendar `time-range` filtering on event start, and minimal task `time-range` filtering on `DUE`
- `GET` returns one `vCard`, `VEVENT`, or `VTODO` object and honors `If-None-Match`
- `PUT` performs full-resource replacement for one `vCard`, `VEVENT`, or `VTODO` object and honors `If-Match` and `If-None-Match`
- `DELETE` removes one contact, event, or task only when the canonical grant or account scope allows delete and honors `If-Match` and `If-None-Match`
- `ETag` values are returned on DAV resources and write responses

## Minimal mapping

### Contacts to `vCard`

`contacts` fields are mapped as follows:

- `id` -> `UID`
- `name` -> `FN`
- `role` -> `TITLE`
- `email` -> `EMAIL`
- `phone` -> `TEL`
- `team` -> `ORG`
- `notes` -> `NOTE`

### Calendar events to `iCalendar`

`calendar_events` fields are mapped as follows:

- `id` -> `UID`
- `date` + `time` -> `DTSTART`
- `time_zone` -> `DTSTART;TZID=...`
- `duration_minutes` -> `DURATION`
- `recurrence_rule` -> `RRULE`
- `title` -> `SUMMARY`
- `location` -> `LOCATION`
- `notes` -> `DESCRIPTION`
- canonical organizer metadata -> `ORGANIZER`
- structured attendee metadata -> `ATTENDEE`
- legacy plain attendee text remains available through `X-LPE-ATTENDEES` only when no structured attendee metadata is stored

The canonical calendar model still belongs to `LPE`. The DAV adapter does not maintain a parallel `VEVENT` store. It serializes and parses a minimal interoperability subset into the canonical event record.

### Tasks to `VTODO`

`tasks` fields are mapped as follows:

- `id` -> `UID`
- `title` -> `SUMMARY`
- `description` -> `DESCRIPTION`
- `status` -> `STATUS`
- `due_at` -> `DUE`
- `completed_at` -> `COMPLETED`
- `updated_at` -> `LAST-MODIFIED`
- `sort_order` -> `X-LPE-SORT-ORDER`

`STATUS` values are normalized as follows:

- `needs-action` <-> `NEEDS-ACTION`
- `in-progress` <-> `IN-PROCESS`
- `completed` <-> `COMPLETED`
- `cancelled` <-> `CANCELLED`

The canonical task model still belongs to `LPE`. The DAV adapter does not maintain a parallel `VTODO` store. It serializes and parses a small interoperability subset directly into the canonical task record.

## Supported MVP scope

- account authentication reuse
- owned default `CardDAV` address-book collection
- owned default `CalDAV` calendar collection
- owned canonical `VTODO` task collections
- same-tenant shared canonical `VTODO` task collections when canonical task-list grants exist
- same-tenant shared address-book and calendar collections when canonical grants exist
- collection discovery through minimal DAV properties
- read access to contacts, events, and tasks through collection and resource endpoints
- full-resource create and update for contacts, events, and tasks through `PUT`
- deletion for contacts, events, and tasks through `DELETE`
- conditional reads and writes through `ETag`, `If-Match`, and `If-None-Match`
- collection `REPORT` filtering through:
  - requested `href` resources for multiget-style requests
  - simple `text-match` filtering on contact, event, and task text fields
  - minimal calendar `time-range` filtering on event start
  - minimal task `time-range` filtering on `DUE`
- calendar serialization and parsing for:
  - `DTSTART`
  - `TZID` on `DTSTART`
  - `DURATION`
  - `RRULE` preservation
  - `ORGANIZER` with `CN`
  - structured `ATTENDEE` lines with `CN`, `ROLE`, `PARTSTAT`, and `RSVP`
- task serialization and parsing for:
  - `SUMMARY`
  - `DESCRIPTION`
  - `STATUS`
  - `DUE`
  - `COMPLETED`
  - `LAST-MODIFIED`
  - `X-LPE-SORT-ORDER`

## Important rules

- the adapter does not duplicate business logic already implemented in storage
- tenant, owner, and grantee scoping continue to be enforced by the canonical `LPE` account model
- DAV compatibility does not create a separate collaboration authority outside `LPE`
- the adapter does not reuse any `Stalwart` code
- the adapter stores organizer and attendee interoperability metadata only in canonical `LPE` event fields; there is still no DAV-only storage layer
- task compatibility reuses the canonical task-list grant model directly; there is still no DAV-only task rights layer
- rights for contacts and calendar events come from the canonical collection-grant model shared with `JMAP`
- rights for tasks come from the canonical `task_list_grants` model shared with `JMAP` and the account APIs

## Known limitations

- the MVP always exposes the owner's default collections and may expose additional same-tenant shared collections
- DAV task collections are published as `/dav/calendars/me/tasks-{task-list-id}/`
- the MVP supports only a minimal subset of DAV discovery and query semantics
- contact updates replace the whole `vCard`; partial patch semantics are not implemented
- calendar updates still replace the whole `VEVENT`
- task updates still replace the whole `VTODO`
- ACLs remain collection-scoped; there is no per-item DAV ACL yet
- cross-tenant sharing is not supported
- calendar recurrence support is limited to preserving one raw `RRULE`; recurrence expansion, exceptions, and detached instances are not implemented
- time-zone support is limited to preserving `TZID` on `DTSTART`; `VTIMEZONE` definitions are not stored or expanded
- `REPORT` filtering remains intentionally small and does not implement the full CardDAV or CalDAV filter grammar
- calendar time-range filtering currently evaluates the canonical event start and does not expand recurrence sets
- task `time-range` filtering currently evaluates canonical `due_at` only
- alarms, free-busy, `VALARM`, and scheduling workflows are not implemented
- `VTODO` recurrence, attendees, alarms, organizers, priorities, categories, percent-complete, and scheduling workflows are not implemented
