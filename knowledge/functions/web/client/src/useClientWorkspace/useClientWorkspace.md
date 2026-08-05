---
type: TypeScript Function
title: useClientWorkspace
resource: web/client/src/useClientWorkspace.ts#L130-L1088
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockElement/addEventListener
  - functions/web/client/src/client-helpers/countFolders
  - functions/web/client/src/client-helpers/filterMessages
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  - functions/web/client/src/client-helpers/filterContacts
  - functions/web/client/src/client-helpers/filterTasks
  - functions/web/client/src/client-helpers/filterNotes
  - functions/web/client/src/client-helpers/filterJournalEntries
  - functions/web/client/src/useClientWorkspace/draftFromMessage
  - functions/web/client/src/useClientWorkspace/buildMessagePayload
  - functions/web/client/src/useClientWorkspace/mapClientError
  - functions/web/client/src/useClientWorkspace/mapSubmitError
  - functions/web/client/src/useClientWorkspace/followupDueIso
  - functions/web/client/src/useClientWorkspace/reminderIso
  called_by:
  - functions/web/client/src/App/App
---

# Signature

`function useClientWorkspace(copy: ClientCopy, authToken: string | null, identity: ClientIdentity | null)`

# Calls

- [addEventListener](../../../../../functions/LPE-CT/web/app/smoke/test/MockElement/addEventListener.md)
- [countFolders](../../../../../functions/web/client/src/client-helpers/countFolders.md)
- [filterMessages](../../../../../functions/web/client/src/client-helpers/filterMessages.md)
- [includes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)
- [filterContacts](../../../../../functions/web/client/src/client-helpers/filterContacts.md)
- [filterTasks](../../../../../functions/web/client/src/client-helpers/filterTasks.md)
- [filterNotes](../../../../../functions/web/client/src/client-helpers/filterNotes.md)
- [filterJournalEntries](../../../../../functions/web/client/src/client-helpers/filterJournalEntries.md)
- [draftFromMessage](../../../../../functions/web/client/src/useClientWorkspace/draftFromMessage.md)
- [buildMessagePayload](../../../../../functions/web/client/src/useClientWorkspace/buildMessagePayload.md)
- [mapClientError](../../../../../functions/web/client/src/useClientWorkspace/mapClientError.md)
- [mapSubmitError](../../../../../functions/web/client/src/useClientWorkspace/mapSubmitError.md)
- [followupDueIso](../../../../../functions/web/client/src/useClientWorkspace/followupDueIso.md)
- [reminderIso](../../../../../functions/web/client/src/useClientWorkspace/reminderIso.md)

# Called by

- [App](../../../../../functions/web/client/src/App/App.md)