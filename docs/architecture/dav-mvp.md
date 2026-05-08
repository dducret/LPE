# DAV Adapter

## Current State/Functionality Overview

`lpe-dav` exposes CardDAV, CalDAV, and task `VTODO` compatibility over canonical `LPE` contacts, calendar events, tasks, and access rules. It does not introduce DAV-specific storage or rights.

## Implementation/Usage

- Endpoints:
  - `/.well-known/carddav`
  - `/.well-known/caldav`
  - `/api/.well-known/carddav`
  - `/api/.well-known/caldav`
  - `/dav/principals/me/`
  - `/dav/addressbooks/me/{collection-id}/`
  - `/dav/calendars/me/{collection-id}/`
  - `/dav/calendars/me/tasks-{task-list-id}/`
- Authentication:
  - mailbox account session from `/api/mail/auth/login`
  - bearer token from `/api/mail/auth/oauth/access-token`
- Supported methods:
  - `OPTIONS`
  - `PROPFIND`
  - `REPORT`
  - `GET`
  - `PUT`
  - `DELETE`
- Contacts:
  - map canonical `contacts` to `vCard`
  - write `vCard` updates back to canonical contacts
- Calendar:
  - map canonical `calendar_events` to `iCalendar` `VEVENT`
  - write supported `VEVENT` fields back to canonical events
- Tasks:
  - map canonical `tasks` to `VTODO`
  - expose one task collection per accessible task list
- Constraints:
  - no DAV-only business model
  - no DAV-only ACL model
  - no WebDAV file-storage surface
  - unsupported properties are ignored unless they are required for safety

## Reference Table/List

| DAV object | Canonical model |
| --- | --- |
| `vCard` | `contacts` |
| `VEVENT` | `calendar_events` |
| `VTODO` | `tasks` |

| Task collection URL | Meaning |
| --- | --- |
| `/dav/calendars/me/tasks-{task-list-id}/` | canonical task list as DAV task collection |
