# DAV MVP

## Objective

This document describes the first `CardDAV` and `CalDAV` adapter implemented in `LPE`.

The `crates/lpe-dav` crate exposes a deliberately small DAV compatibility layer for contacts and calendar data without changing the canonical collaboration model centered on `LPE`.

## Architectural principles

- `JMAP` remains the primary modern protocol axis
- `CardDAV` and `CalDAV` are compatibility adapters only
- mailbox-account authentication is reused rather than duplicated
- `contacts` remains the source of truth for address-book data
- `calendar_events` remains the source of truth for calendar data
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
- `REPORT` returns the full current resources for address-book and calendar collections
- `GET` returns one `vCard` or one `iCalendar` object
- `PUT` performs full-resource replacement for one `vCard` or one `iCalendar` object
- `DELETE` removes one contact or one event owned by the authenticated account

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
- `title` -> `SUMMARY`
- `location` -> `LOCATION`
- `notes` -> `DESCRIPTION`
- `attendees` -> `X-LPE-ATTENDEES`

## Supported MVP scope

- account authentication reuse
- default `CardDAV` address-book collection
- default `CalDAV` calendar collection
- collection discovery through minimal DAV properties
- read access to contacts and events through collection and resource endpoints
- full-resource create and update for contacts and events through `PUT`
- deletion for contacts and events through `DELETE`

## Important rules

- the adapter does not duplicate business logic already implemented in storage
- tenant and account scoping continue to be enforced by the canonical `LPE` account model
- DAV compatibility does not create a separate collaboration authority outside `LPE`
- the adapter does not reuse any `Stalwart` code

## Known limitations

- the MVP exposes one default address-book collection and one default calendar collection per account
- the MVP supports only a minimal subset of DAV discovery and query semantics
- `REPORT` currently returns the current collection members without implementing full DAV filtering
- contact updates replace the whole `vCard`; partial patch semantics are not implemented
- calendar updates replace the whole `VEVENT`; recurrence, alarms, organizers, attendees as structured entities, and time zones are not implemented
- calendar attendee data is currently preserved through `X-LPE-ATTENDEES` because the canonical model still stores attendees as plain text
- `ETag` values are derived from the serialized DAV payload and do not yet support conditional writes
