---
type: TypeScript Function
title: filterMessages
resource: web/client/src/client-helpers.ts#L83-L92
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  called_by:
  - functions/web/client/src/useClientWorkspace/useClientWorkspace
---

# Signature

`function filterMessages(messages: Message[], folder: Folder, query: string): Message[]`

# Calls

- [includes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)

# Called by

- [useClientWorkspace](../../../../../functions/web/client/src/useClientWorkspace/useClientWorkspace.md)