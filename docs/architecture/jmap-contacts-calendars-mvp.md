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
  - `ContactCard/get`
  - `ContactCard/set`
  - `ContactCard/changes`
  - `ContactCard/query`
  - `ContactCard/queryChanges`
  - `Calendar/get`
  - `CalendarEvent/get`
  - `CalendarEvent/set`
  - `CalendarEvent/changes`
  - `CalendarEvent/query`
  - `CalendarEvent/queryChanges`
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
- Mapping:
  - one canonical `default` address book per account
  - one canonical `default` calendar per account
  - contacts map to canonical `contacts`
  - events map to canonical `calendar_events`
  - private `Note` maps to canonical `notes`
  - private `JournalEntry` maps to canonical `journal_entries`
  - private `Reminder` is computed from reminder-bearing canonical tasks and calendar events
- Push:
  - private `Note` and `JournalEntry` are WebSocket push data types
  - their state changes are driven by canonical `notes` and `journal`
    categories, not protocol-local sync state
- Rules:
  - rights are bounded by authenticated account and canonical grants
  - no JMAP-only collection store
  - no protocol-local sharing model
  - query/change behavior uses canonical timestamps and state
  - private Outlook compatibility methods must not overload JMAP Mail `Mailbox` or `Email`

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
