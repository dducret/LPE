---
type: TypeScript Function
title: buildMessagePayload
resource: web/client/src/useClientWorkspace.ts#L80-L105
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/web/client/src/useClientWorkspace/splitRecipients
  called_by:
  - functions/web/client/src/useClientWorkspace/useClientWorkspace
---

# Signature

`function buildMessagePayload( identity: ClientIdentity, mailbox: MailboxAccountAccess, draft: MessageDraft, draftMessageId: string | null )`

# Calls

- [splitRecipients](../../../../../functions/web/client/src/useClientWorkspace/splitRecipients.md)

# Called by

- [useClientWorkspace](../../../../../functions/web/client/src/useClientWorkspace/useClientWorkspace.md)