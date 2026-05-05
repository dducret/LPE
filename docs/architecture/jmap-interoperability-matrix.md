# JMAP Interoperability Matrix

### Objective

This matrix defines the current `JMAP` interoperability target for `LPE`.

The release target is the `JMAP` Big Three: Mail, Contacts, and simple Calendar events. The goal is not broader method count; it is predictable behavior for client discovery, request batching, state refresh, reconnect, and canonical storage consistency.

### Target clients and harnesses

- `JMAP::Tester` is the external black-box protocol harness for request/response shape, JSON type stability, upload/download behavior, method batching, and state-change assertions.
- Fastmail-style client behavior is the real-client reference for session discovery, bearer-token authentication, capability scoping, typed JSON responses, and practical Mail plus Contacts workflows.
- Fastmail Calendar-over-JMAP behavior is not treated as a production reference until its public API support is finalized; `LPE` keeps simple Calendar event interoperability bounded to the implemented JMAP Calendars shape and canonical `calendar_events` rows.

`JMAP::Tester` is not vendored and is not a repository dependency. It remains an operator-run external harness because the repository dependency policy only allows Apache-2.0 code and documented MIT exceptions. In-repo Rust tests should still mimic the key `JMAP::Tester` assertions: exact method-response order, call-id preservation, JSON string/number/boolean shapes, capability-gated dispatch, and canonical state transitions.

### Mail cases

- session document advertises only implemented and exposed endpoints
- `Mailbox/query` and `Mailbox/get` return stable typed JSON for default and delegated mailbox accounts
- `Email/query`, `Email/get`, `Email/changes`, and `Email/queryChanges` preserve coherent state under create, update, move, copy, import, submit, and delete flows
- `Email/get`, snippets, query projections, and downloads do not expose protected `Bcc` to delegated readers
- `EmailSubmission/set`, `Identity/get`, and submission changes use canonical sender delegation and submission rights
- `Blob/upload`, `Blob/get`, `Blob/copy`, `Blob/lookup`, and HTTP download interoperate with upload IDs and canonical message blob IDs without creating a separate blob store
- WebSocket `StateChange` replay falls back to full snapshot when the bounded journal cannot cover the requested cursor

### Contacts Cases

- `AddressBook/get`, `AddressBook/query`, `AddressBook/changes`, and `AddressBook/queryChanges` expose canonical collections and rights
- `ContactCard/get`, `ContactCard/query`, `ContactCard/changes`, `ContactCard/queryChanges`, and `ContactCard/set` map directly to canonical contacts
- shared collections stay bounded by canonical same-tenant grants

### Calendar Cases

- `Calendar/get`, `Calendar/query`, `Calendar/changes`, and `Calendar/queryChanges` expose canonical calendar collections and rights
- `CalendarEvent/get`, `CalendarEvent/query`, `CalendarEvent/changes`, `CalendarEvent/queryChanges`, and `CalendarEvent/set` cover simple events, organizer metadata, attendee status, and RSVP intent stored on canonical event rows
- complex scheduling workflows, cross-account calendar movement, and invitation delivery semantics are deferred

### Deferred

- durable query-history storage for long-lived search/query cursors
- complex cross-account data movement
- broader `JMAP` family expansion beyond Mail, Contacts, and simple Calendar events
- treating `JMAP Tasks` as a primary external interoperability target for this release
