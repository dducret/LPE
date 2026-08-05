---
type: Rust Module
title: protocol
resource: crates/lpe-jmap/src/protocol.rs#L1-L627
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/serde-deserialize-serialize
  - external/serde-json-value
  - external/std-collections-hashmap
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [JmapApiRequest](../../../../classes/crates/lpe-jmap/src/protocol/JmapApiRequest.md)
- [JmapMethodCall](../../../../classes/crates/lpe-jmap/src/protocol/JmapMethodCall.md)
- [JmapApiResponse](../../../../classes/crates/lpe-jmap/src/protocol/JmapApiResponse.md)
- [JmapMethodResponse](../../../../classes/crates/lpe-jmap/src/protocol/JmapMethodResponse.md)
- [WebSocketRequestEnvelope](../../../../classes/crates/lpe-jmap/src/protocol/WebSocketRequestEnvelope.md)
- [WebSocketPushEnable](../../../../classes/crates/lpe-jmap/src/protocol/WebSocketPushEnable.md)
- [WebSocketPushDisable](../../../../classes/crates/lpe-jmap/src/protocol/WebSocketPushDisable.md)
- [WebSocketResponse](../../../../classes/crates/lpe-jmap/src/protocol/WebSocketResponse.md)
- [WebSocketRequestError](../../../../classes/crates/lpe-jmap/src/protocol/WebSocketRequestError.md)
- [WebSocketStateChange](../../../../classes/crates/lpe-jmap/src/protocol/WebSocketStateChange.md)
- [SessionDocument](../../../../classes/crates/lpe-jmap/src/protocol/SessionDocument.md)
- [SessionAccount](../../../../classes/crates/lpe-jmap/src/protocol/SessionAccount.md)
- [MailboxGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/MailboxGetArguments.md)
- [MailboxQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/MailboxQueryArguments.md)
- [QueryChangesArguments](../../../../classes/crates/lpe-jmap/src/protocol/QueryChangesArguments.md)
- [MailboxSetArguments](../../../../classes/crates/lpe-jmap/src/protocol/MailboxSetArguments.md)
- [ChangesArguments](../../../../classes/crates/lpe-jmap/src/protocol/ChangesArguments.md)
- [EmailGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/EmailGetArguments.md)
- [EmailQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/EmailQueryArguments.md)
- [EmailQueryFilter](../../../../classes/crates/lpe-jmap/src/protocol/EmailQueryFilter.md)
- [EmailQuerySort](../../../../classes/crates/lpe-jmap/src/protocol/EmailQuerySort.md)
- [EmailSetArguments](../../../../classes/crates/lpe-jmap/src/protocol/EmailSetArguments.md)
- [EmailSubmissionSetArguments](../../../../classes/crates/lpe-jmap/src/protocol/EmailSubmissionSetArguments.md)
- [EmailSubmissionGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/EmailSubmissionGetArguments.md)
- [EmailSubmissionQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/EmailSubmissionQueryArguments.md)
- [EmailSubmissionQueryFilter](../../../../classes/crates/lpe-jmap/src/protocol/EmailSubmissionQueryFilter.md)
- [EmailSubmissionQuerySort](../../../../classes/crates/lpe-jmap/src/protocol/EmailSubmissionQuerySort.md)
- [IdentityGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/IdentityGetArguments.md)
- [ThreadGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/ThreadGetArguments.md)
- [ThreadQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/ThreadQueryArguments.md)
- [SearchSnippetGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/SearchSnippetGetArguments.md)
- [EmailCopyArguments](../../../../classes/crates/lpe-jmap/src/protocol/EmailCopyArguments.md)
- [EmailImportArguments](../../../../classes/crates/lpe-jmap/src/protocol/EmailImportArguments.md)
- [QuotaGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/QuotaGetArguments.md)
- [AddressBookGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/AddressBookGetArguments.md)
- [AddressBookQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/AddressBookQueryArguments.md)
- [ContactCardGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/ContactCardGetArguments.md)
- [ContactCardQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/ContactCardQueryArguments.md)
- [ContactCardQueryFilter](../../../../classes/crates/lpe-jmap/src/protocol/ContactCardQueryFilter.md)
- [ContactCardSetArguments](../../../../classes/crates/lpe-jmap/src/protocol/ContactCardSetArguments.md)
- [RecipientSuggestionQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/RecipientSuggestionQueryArguments.md)
- [CalendarGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/CalendarGetArguments.md)
- [CalendarQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/CalendarQueryArguments.md)
- [CalendarSetArguments](../../../../classes/crates/lpe-jmap/src/protocol/CalendarSetArguments.md)
- [TaskListGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/TaskListGetArguments.md)
- [TaskListSetArguments](../../../../classes/crates/lpe-jmap/src/protocol/TaskListSetArguments.md)
- [TaskGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/TaskGetArguments.md)
- [TaskQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/TaskQueryArguments.md)
- [TaskQueryFilter](../../../../classes/crates/lpe-jmap/src/protocol/TaskQueryFilter.md)
- [TaskQuerySort](../../../../classes/crates/lpe-jmap/src/protocol/TaskQuerySort.md)
- [TaskSetArguments](../../../../classes/crates/lpe-jmap/src/protocol/TaskSetArguments.md)
- [NoteGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/NoteGetArguments.md)
- [NoteQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/NoteQueryArguments.md)
- [NoteQueryFilter](../../../../classes/crates/lpe-jmap/src/protocol/NoteQueryFilter.md)
- [NoteSetArguments](../../../../classes/crates/lpe-jmap/src/protocol/NoteSetArguments.md)
- [JournalEntryGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/JournalEntryGetArguments.md)
- [JournalEntryQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/JournalEntryQueryArguments.md)
- [JournalEntryQueryFilter](../../../../classes/crates/lpe-jmap/src/protocol/JournalEntryQueryFilter.md)
- [JournalEntrySetArguments](../../../../classes/crates/lpe-jmap/src/protocol/JournalEntrySetArguments.md)
- [ReminderQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/ReminderQueryArguments.md)
- [CalendarEventGetArguments](../../../../classes/crates/lpe-jmap/src/protocol/CalendarEventGetArguments.md)
- [CalendarEventQueryArguments](../../../../classes/crates/lpe-jmap/src/protocol/CalendarEventQueryArguments.md)
- [CalendarEventQueryFilter](../../../../classes/crates/lpe-jmap/src/protocol/CalendarEventQueryFilter.md)
- [CalendarEventSetArguments](../../../../classes/crates/lpe-jmap/src/protocol/CalendarEventSetArguments.md)
- [EntityQuerySort](../../../../classes/crates/lpe-jmap/src/protocol/EntityQuerySort.md)
- [MailboxCreateInput](../../../../classes/crates/lpe-jmap/src/protocol/MailboxCreateInput.md)
- [MailboxUpdateInput](../../../../classes/crates/lpe-jmap/src/protocol/MailboxUpdateInput.md)
- [DraftMutation](../../../../classes/crates/lpe-jmap/src/protocol/DraftMutation.md)
- [EmailAddressInput](../../../../classes/crates/lpe-jmap/src/protocol/EmailAddressInput.md)

# Imports

- `serde::{Deserialize, Serialize}`
- `serde_json::Value`
- `std::collections::HashMap`
- `uuid::Uuid`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)