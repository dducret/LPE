This directory contains the browser-side modules used by the LPE-CT management console.

Extraction is incremental. Each subdirectory groups one responsibility so `app.js`
can stay as the entry point and orchestration layer.

- `i18n/`: localized message catalogs and locale resolution.
- `pages/`: page modules for the management shell. Each module owns a top-level
  page id, the section ids it contains, and the renderer keys used by `app.js`.
