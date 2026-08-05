---
type: TypeScript Function
title: filterContacts
resource: web/client/src/client-helpers.ts#L94-L100
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  called_by:
  - functions/web/client/src/useClientWorkspace/useClientWorkspace
---

# Signature

`function filterContacts(contacts: ContactItem[], contactBook: ContactBookId, query: string): ContactItem[]`

# Calls

- [includes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)

# Called by

- [useClientWorkspace](../../../../../functions/web/client/src/useClientWorkspace/useClientWorkspace.md)