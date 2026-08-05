---
type: TypeScript Function
title: isLocale
resource: web/shared/src/i18n.ts#L24-L26
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  called_by:
  - functions/web/shared/src/i18n/getInitialLocale
---

# Signature

`function isLocale(value: string | null | undefined): value is Locale`

# Calls

- [includes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)

# Called by

- [getInitialLocale](../../../../../functions/web/shared/src/i18n/getInitialLocale.md)