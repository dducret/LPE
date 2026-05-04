# JMAP Contacts and Calendars MVP

### Objective

This document describes the `JMAP Contacts` and `JMAP Calendars` scope currently supported by `LPE` for the MVP.

The `crates/lpe-jmap` crate acts as a `JMAP` adapter on top of the canonical models already exposed through `lpe-storage`, `CardDAV`, `CalDAV`, and `ActiveSync`. It does not introduce any parallel storage, rights, or business logic for contacts and calendars.

### Architectural principles

- `JMAP` remains the primary modern axis for collaboration data
- `contacts` remains the source of truth for contact cards
- `calendar_events` remains the source of truth for calendar events
- `CardDAV`, `CalDAV`, `ActiveSync`, and `JMAP` converge on the same canonical objects
- rights are derived from the canonical same-tenant grant model; there is no `JMAP`-specific sharing or delegation model
- no `Stalwart` code is reused

### Authentication

- the `JMAP` client reuses the existing mailbox-account authentication
- login remains `/api/mail/auth/login`
- the existing account bearer token must then be sent to `/api/jmap/session` and `/api/jmap/api`
- without the Debian reverse proxy, the same routes are exposed directly as `/jmap/session` and `/jmap/api`
- the Debian reverse proxy sets `X-Forwarded-Prefix: /api/jmap` so the shared `Session` document advertises public `/api/jmap/*` URLs

### Supported session capabilities

- `urn:ietf:params:jmap:core`
- `urn:ietf:params:jmap:contacts`
- `urn:ietf:params:jmap:calendars`

The `JMAP` session exposes the authenticated mailbox account as the primary account and may expose additional accessible shared mailbox accounts in the session account map. Address books and calendars are exposed as stable canonical collections inside the same tenant, with owned and shared access resolved from the same underlying collection rights.

### Supported methods

Contacts:

- `AddressBook/get`
- `AddressBook/query`
- `AddressBook/changes`
- `ContactCard/get`
- `ContactCard/query`
- `ContactCard/changes`
- `ContactCard/set`

Calendars:

- `Calendar/get`
- `Calendar/query`
- `Calendar/changes`
- `CalendarEvent/get`
- `CalendarEvent/query`
- `CalendarEvent/changes`
- `CalendarEvent/set`

### MVP mapping

#### Address book

- the owner sees its canonical `default` address book
- extra shared address books may be exposed for same-tenant grants
- `myRights` advertises read, write, delete, and share access according to the resolved canonical grant
- `mayCreateTopLevel` remains `false`

#### ContactCard

The `JMAP` to `contacts` mapping is:

- `id` and `uid` -> `contacts.id`
- `name.full` -> `contacts.name`
- `titles.*.name` -> `contacts.role`
- `emails.*.address` -> `contacts.email`
- `phones.*.number` -> `contacts.phone`
- `organizations.*.name` -> `contacts.team`
- `notes.*.note` -> `contacts.notes`
- `addressBookIds.{collectionId}` -> an owned or shared canonical collection resolved through the canonical ACL model

#### Calendar

- the owner sees its canonical `default` calendar
- extra shared calendars may be exposed for same-tenant grants
- `myRights` advertises read, write, delete, and share access according to the resolved canonical grant
- `mayCreateTopLevel` remains `false`

#### CalendarEvent

The `JMAP` to `calendar_events` mapping is:

- `id` and `uid` -> `calendar_events.id`
- `title` -> `calendar_events.title`
- `start` (`YYYY-MM-DDTHH:MM:SS`) -> `calendar_events.date` + `calendar_events.time`
- `locations.*.name` -> `calendar_events.location`
- `participants` -> canonical structured participant metadata stored in `calendar_events.attendees_json`, with attendee labels mirrored into `calendar_events.attendees`
- `description` -> `calendar_events.notes`
- `calendarIds.{collectionId}` -> an owned or shared canonical collection resolved through the canonical ACL model

Organizer and participant status are exposed through `participants`:

- one participant with `roles.owner=true` is treated as the canonical organizer for the event
- attendee participants keep their `participationStatus` and `expectReply` flags in the canonical event metadata
- attendee labels remain mirrored into `calendar_events.attendees` so older filters and compatibility fallbacks keep working

### Important MVP rules

- `ContactCard/set` directly creates, replaces, or deletes canonical `contacts` rows
- `CalendarEvent/set` directly creates, replaces, or deletes canonical `calendar_events` rows
- `JMAP` reads use the same canonical objects as `CardDAV`, `CalDAV`, and `ActiveSync`
- organizer and attendee status updates are stored only in canonical `calendar_events` metadata; there is no `JMAP`-only scheduling state
- the owner keeps full rights on its `default` canonical collection
- an account in the same tenant may read or mutate a shared collection only through a canonical grant
- cross-tenant access is not supported
- rights changes are minimally audited through `audit_events`
- `changes` re-exposes the current account state and does not yet maintain a fine-grained sync history

### Accepted MVP limitations

- an owning account always has a durable `default` canonical collection; extra shared collections may also be exposed
- ACLs remain at the implicit collection level; there is no per-contact or per-event ACL yet
- sharing and delegation remain limited to the same tenant
- `ContactCard/query` supports only ascending `name` sort and simple text filtering, with `inAddressBook` limited to one target collection
- `CalendarEvent/query` supports only ascending `start` sort and the `inCalendar`, `text`, `after`, and `before` filters
- `ContactCard/set` supports only `kind=individual`
- `CalendarEvent/set` supports only `@type=Event`
- `CalendarEvent/set` accepts only `duration=PT0S`
- `timeZone` must be `null` or absent
- updates are full-resource replacements; fine-grained property patches are not implemented
- calendar participants are still mirrored as text for legacy fallback, but the canonical event metadata now stores one organizer plus attendee status and RSVP intent for interoperable adapters
- no recurrence, alarms, free/busy, calendar attachments, or extended `VCARD` or `VCALENDAR` semantics
- `AddressBook` and `Calendar` objects represent durable canonical collections and cannot be modified through `set` in the MVP
- no user-specific renaming of shared collections
- no fine-grained ACL history or real-time rights-change notifications

### Consistency with other adapters

- `CardDAV` and `CalDAV` remain DAV compatibility layers over the same canonical tables
- `ActiveSync` remains the native Outlook/mobile layer over the same canonical tables
- `JMAP Contacts` and `JMAP Calendars` become the primary modern access path over those same models
- mailbox delegation and sender authorization for `JMAP Mail` also reuse the same canonical mailbox-access and canonical submission model; there is no `JMAP`-specific shared-mailbox or sender-right state


