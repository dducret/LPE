# DAV MVP

## Objective

This document describes the current `CardDAV` and `CalDAV` interoperability adapter implemented in `LPE`.

The `crates/lpe-dav` crate exposes a deliberately small but more interoperable DAV compatibility layer for contacts and calendar data without introducing a DAV-specific storage model. It remains an adapter over the canonical collaboration data already owned by `LPE`.

## Architectural principles

- `JMAP` remains the primary modern protocol axis
- `CardDAV` and `CalDAV` are compatibility adapters only
- mailbox-account authentication is reused rather than duplicated
- `contacts` remains the source of truth for address-book data
- `calendar_events` remains the source of truth for calendar data, including the minimal interoperability metadata now needed for DAV clients
- future task compatibility must reuse the canonical `tasks` model rather than introducing DAV-only task storage
- the adapter must not introduce a separate storage, tenant, or rights model
- each DAV request is scoped to the authenticated account only

## Endpoints

- `/.well-known/carddav`
- `/.well-known/caldav`
- `/dav/`
- `/dav/principals/me/`
- `/dav/addressbooks/me/default/`
- `/dav/calendars/me/default/`

Without the Debian reverse proxy, these routes are exposed directly by the Rust service.

With the documented `/api/` reverse proxy, they are reachable through `/api/.well-known/carddav`, `/api/.well-known/caldav`, and `/api/dav/...`.

## Authentication

- mailbox-account `Bearer` sessions created by `/api/mail/auth/login` are accepted
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

- `PROPFIND` exposes the root, current principal, the default address-book collection, the default calendar collection, and collection members
- `REPORT` supports collection reads, multiget-style `href` targeting, simple text-match filtering, and minimal calendar `time-range` filtering on event start
- `GET` returns one `vCard` or one `iCalendar` object and honors `If-None-Match`
- `PUT` performs full-resource replacement for one `vCard` or one `iCalendar` object and honors `If-Match` and `If-None-Match`
- `DELETE` removes one contact or one event owned by the authenticated account and honors `If-Match` and `If-None-Match`
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
- structured attendee metadata -> `ATTENDEE`
- legacy plain attendee text remains available through `X-LPE-ATTENDEES` only when no structured attendee metadata is stored

The canonical calendar model still belongs to `LPE`. The DAV adapter does not maintain a parallel `VEVENT` store. It serializes and parses a minimal interoperability subset into the canonical event record.

## Supported MVP scope

- account authentication reuse
- default `CardDAV` address-book collection
- default `CalDAV` calendar collection
- collection discovery through minimal DAV properties
- read access to contacts and events through collection and resource endpoints
- full-resource create and update for contacts and events through `PUT`
- deletion for contacts and events through `DELETE`
- conditional reads and writes through `ETag`, `If-Match`, and `If-None-Match`
- collection `REPORT` filtering through:
  - requested `href` resources for multiget-style requests
  - simple `text-match` filtering on contact and event text fields
  - minimal calendar `time-range` filtering on event start
- calendar serialization and parsing for:
  - `DTSTART`
  - `TZID` on `DTSTART`
  - `DURATION`
  - `RRULE` preservation
  - structured `ATTENDEE` lines with `CN`, `ROLE`, `PARTSTAT`, and `RSVP`

## Important rules

- the adapter does not duplicate business logic already implemented in storage
- tenant and account scoping continue to be enforced by the canonical `LPE` account model
- DAV compatibility does not create a separate collaboration authority outside `LPE`
- the adapter does not reuse any `Stalwart` code
- the adapter stores interoperability metadata only in canonical `LPE` event fields; there is still no DAV-only storage layer

## Known limitations

- the MVP exposes one default address-book collection and one default calendar collection per account
- the MVP supports only a minimal subset of DAV discovery and query semantics
- contact updates replace the whole `vCard`; partial patch semantics are not implemented
- calendar updates still replace the whole `VEVENT`
- calendar recurrence support is limited to preserving one raw `RRULE`; recurrence expansion, exceptions, and detached instances are not implemented
- time-zone support is limited to preserving `TZID` on `DTSTART`; `VTIMEZONE` definitions are not stored or expanded
- `REPORT` filtering remains intentionally small and does not implement the full CardDAV or CalDAV filter grammar
- calendar time-range filtering currently evaluates the canonical event start and does not expand recurrence sets
- organizers, alarms, free-busy, `VALARM`, and scheduling workflows are not implemented
- `VTODO` and task collections are intentionally out of scope; future task compatibility must build on `docs/architecture/tasks-mvp.md`
