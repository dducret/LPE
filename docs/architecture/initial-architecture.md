# Initial Architecture

## Current State/Functionality Overview

`LPE` is a multi-tenant mail and collaboration platform with a strict split between the core server and the `LPE-CT` DMZ sorting center. Core state lives in `LPE`; Internet mail transport and perimeter enforcement live in `LPE-CT`.

## Implementation/Usage

- Core `LPE` responsibilities:
  - domains
  - accounts
  - aliases
  - quotas
  - mailboxes
  - contacts
  - calendars
  - tasks
  - notes
  - journal entries
  - computed reminders
  - rights
  - storage
  - search
  - canonical `Sent`
  - user-visible state
- `LPE-CT` responsibilities:
  - Internet `SMTP` ingress
  - outbound relay
  - authenticated outbound handoff reception from `LPE`
  - authenticated final delivery toward `LPE`
  - DKIM signing
  - SPF and DMARC policy handling
  - retries
  - outbound queue behavior
  - bounces and `DSN`
  - quarantine
  - perimeter security
- Architectural rules:
  - source code license is `Apache-2.0`
  - dependencies follow `LICENSE.md`
  - `PostgreSQL` is the primary store
  - `JMAP` is the primary modern protocol
  - `IMAP` is a mailbox compatibility layer
  - `ActiveSync` targets mobile/native clients that support `Exchange ActiveSync`
  - `EWS` is bounded Exchange compatibility over canonical state
  - `MAPI over HTTP` is guarded and authenticated
  - `DAV` exposes collaboration compatibility over canonical state
  - `ManageSieve` manages mailbox scripts executed during final delivery
  - client autodiscovery must publish only implemented and exposed endpoints
  - no adapter may implement parallel `Sent` or `Outbox` logic
- Canonical submission flow at `/api/mail/messages/submit`:
  - verify the submitting account
  - ensure the `Sent` mailbox exists
  - create the canonical message in `messages`
  - store visible recipients in `message_recipients`
  - retain `Bcc` in protected storage
  - index the body in `message_bodies` without `Bcc` in `participants_normalized`
  - add a `submission_queue` entry for `LPE-CT`
  - record the action in `audit_events`
- Webmail:
  - `/mail/` authenticates through `/api/mail/auth/login`
  - `/api/mail/auth/me` exposes session identity
  - `/api/mail/workspace` loads persistent account workspace state
  - drafts are persistent messages in the `Drafts` mailbox
- File validation:
  - every external or client-provided file must be validated with Google `Magika`
- Internationalization:
  - web interfaces support at least `en`, `fr`, `de`, `it`, and `es`
  - English is the default UI language

## Reference Table/List

| Crate / component | Role |
| --- | --- |
| `lpe-domain` | shared business types |
| `lpe-core` | application rules and orchestration |
| `lpe-storage` | PostgreSQL persistence, blob storage support, mail parsing helpers |
| `lpe-ai` | local AI contracts and provenance |
| `lpe-jmap` | JMAP adapter |
| `lpe-admin-api` | administration control plane |
| `lpe-cli` | local server executable |
| `lpe-imap` | IMAP compatibility adapter |
| `lpe-activesync` | ActiveSync compatibility adapter |
| `lpe-exchange` | EWS and MAPI compatibility adapter |
| `lpe-dav` | DAV compatibility adapter |
| `LPE-CT` | DMZ sorting center |

| Endpoint | Canonical purpose |
| --- | --- |
| `/api/mail/messages/submit` | canonical submission |
| `/api/mail/auth/login` | mailbox login |
| `/api/mail/auth/me` | mailbox session identity |
| `/api/mail/workspace` | workspace snapshot |
| `/api/mail/contact-books` | accessible canonical contact-book list |
| `/api/mail/contacts` | canonical contact list and create API |
| `/api/mail/contacts/{contact_id}` | canonical contact fetch, patch, and delete API |
| `/api/mail/recipient-suggestions` | private owner-scoped compose recipient suggestions |
| `/api/mail/recipient-suggestions/{suggestion_id}/dismiss` | dismiss a private owner-scoped recipient suggestion |
| `/api/mail/tasks` | task API |
| `/api/mail/notes` | canonical notes API |
| `/api/mail/journal` | canonical journal API |
| `/api/mail/reminders` | computed reminders API |
| `/api/mail/search-folders` | canonical Outlook Search Folder definition API |
| `/api/mail/recoverable-items` | canonical recoverable message browse, restore, and purge API |
| `/api/mail/public-folders/trees` | canonical public-folder tree discovery API |
| `/api/mail/public-folders/{folderId}` | canonical public-folder folder, item, permission, replica topology, and per-user state API root |
| `/api/mail/rules` | read-only Outlook-compatible mailbox rule projection backed by Sieve scripts |
| `/api/mail/outlook-profile` | read-only server-side Outlook profile state summary derived from canonical tables |
