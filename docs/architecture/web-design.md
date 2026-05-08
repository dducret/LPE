# Web Design

## Current State/Functionality Overview

`LPE` web interfaces use a shared Tailwind-based design system under `web/shared` and `web/ui`. New web work must converge on shared tokens, presets, and primitives instead of app-local styling.

## Implementation/Usage

- Technology base:
  - shared theme tokens in `web/shared`
  - shared `Tailwind CSS` preset in `web/ui/tailwind`
  - shared primitives in `web/ui/src/components/primitives`
- Required languages:
  - `en`
  - `fr`
  - `de`
  - `it`
  - `es`
  - default: English
- Shared UI rules:
  - use shared primitives for buttons, inputs, selects, textareas, badges, cards, drawers, and tabs
  - use right-side drawers for management create/detail/context actions
  - use full-width list layouts for administration lists
  - keep primary `New` or `Create` action in list headers
  - do not ship mock datasets, placeholder marketing copy, or nonfunctional placeholder actions in runtime UI
  - avoid app-local root token duplication
- Visual rules:
  - keep administration UI dense, quiet, and workflow-focused
  - keep client UI readable and message-focused
  - avoid nested cards
  - avoid one-off utility sprawl
  - ensure mobile and desktop text does not overflow its container
  - use stable dimensions for fixed-format controls and grids
  - do not scale font size with viewport width
  - keep letter spacing at `0`
- Accessibility:
  - preserve keyboard operation
  - preserve focus states
  - meet contrast requirements
  - support responsive layouts without overlapping controls
- Reference previews:
  - `docs/architecture/admin-web-design-preview-tailwind.html`
  - `docs/architecture/client-web-design-preview-tailwind.html`

## Reference Table/List

| Path | Purpose |
| --- | --- |
| `web/shared` | shared theme tokens |
| `web/shared/src/theme.css` | token CSS |
| `web/ui` | shared UI package |
| `web/ui/tailwind` | Tailwind preset |
| `web/ui/src/components/primitives` | primitive components |
| `docs/architecture/admin-web-design-preview-tailwind.html` | admin visual reference |
| `docs/architecture/client-web-design-preview-tailwind.html` | client visual reference |
