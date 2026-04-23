# Web Design

### Goal

This document defines the shared web-design direction for `LPE`.

It merges the previous administration UI guidance and the shared `Tailwind CSS` design-system guidance into one architecture reference for:

- `web/admin`
- `web/client`

The goal is to keep one coherent web language across products while allowing density and workflow differences between administration and end-user experiences.

### When to use this document

This document must be treated as the reference architecture for any task that changes:

- layout
- navigation
- component styling
- responsive behavior
- drawers, dialogs, overlays, and toolbars
- shared design tokens or `Tailwind` conventions
- administration or client interaction patterns

### Technology and normalization base

The current normalization base for `LPE` web interfaces is a shared `Tailwind CSS` preset and primitive layer under `web/ui`, anchored on the shared theme tokens in `web/shared`.

Today that means:

- shared CSS design tokens and base interaction rules in `web/shared/src/theme.css`
- shared `Tailwind CSS` preset and utility layer in `web/ui/tailwind`
- shared primitive React components in `web/ui/src/components/primitives`
- app-local CSS and feature markup that may still layer product-specific aliases and layouts on top of those shared primitives and tokens

The convergence model remains:

- one shared theme
- reusable primitives
- documented variants
- domain-level components built on top of those primitives

The visual target is a contemporary `premium SaaS` rendering:

- light semi-transparent surfaces
- restrained `glassmorphism`
- `backdrop blur`
- subtle edge highlights
- richer semantic badges
- restrained micro-interactions
- organic motion

### Shared design-system rules

The shared design system must provide:

- centralized tokens for color, spacing, radius, shadows, focus, and semantic states
- shared component primitives for `Button`, `Input`, `Textarea`, `Select`, `Badge`, `Card`, `Drawer`, `Dialog`, `Tabs`, `Table`, `Toolbar`, `Sidebar`, `EmptyState`, `Avatar`, `Dropdown`, and `Toast`
- reusable variants instead of repeated long utility strings
- a `cn()`-style composition helper and reusable variant logic

The current codebase still only partially implements that target. New web work must converge on the shared token layer, `Tailwind` preset, and primitive components instead of duplicating root token sets or inventing one-off control styling inside each app.

The shared theme should cover at least:

- neutral colors
- semantic colors
- typography scale
- spacing scale
- radii
- shadow presets
- breakpoints
- focus treatments
- surface transparency levels
- `backdrop blur` levels
- inset `ring` treatments for premium surfaces
- standard transition curves, including an organic `spring`-like curve

Recommended normalized semantic tokens include:

- `bg`
- `panel`
- `panel-soft`
- `border`
- `text`
- `text-soft`
- `primary`
- `success`
- `warning`
- `danger`

Recommended normalized variants include:

- `Button`: `primary`, `secondary`, `ghost`, `danger`
- `Badge`: `info`, `success`, `warning`, `danger`, `neutral`
- `Card`: `default`, `soft`, `elevated`, `interactive`, `premium`
- `Drawer`: `default`, `wide`, `form`, `details`
- `Table`: `compact`, `default`, `comfortable`
- `Panel`: `default`, `glass`
- `Search`: `default`, `focused`
- `Sidebar`: `default`, `collapsed`

### Repository structure target

The recommended structure is:

```text
web/
  ui/
    tailwind/
      preset.ts
      tokens.ts
      utilities.css
    src/
      components/
        primitives/
          Button.tsx
          Input.tsx
          Badge.tsx
          Card.tsx
          Drawer.tsx
          Table.tsx
          Toolbar.tsx
          Sidebar.tsx
          EmptyState.tsx
          Tabs.tsx
          Dialog.tsx
        layout/
          AdminShell.tsx
          ClientShell.tsx
          PageHeader.tsx
          SectionCard.tsx
      lib/
        cn.ts
        variants.ts
  admin/
    tailwind.config.ts
    src/
      app/
      features/
      pages/
      main.tsx
      styles.css
  client/
    tailwind.config.ts
    src/
      app/
      features/
      pages/
      components/
      main.tsx
      styles.css
  shared/
    src/
      theme.css
      i18n.ts
```

This structure is now the implemented baseline for shared web normalization, even though some feature views still carry legacy app-local CSS during the migration.

### Shared shell rules

Both `web/admin` and `web/client` should feel like part of the same product family.

Shared shell expectations:

- fixed left sidebar as the main structural navigation element
- collapsible sidebar in desktop layouts
- top toolbar for search, context, help, and profile actions
- premium light surfaces with restrained depth
- right-side drawers for contextual actions when appropriate

Implementation rules:

- sidebar collapse should prefer width transition on the side panel rather than abrupt whole-grid rewrites
- collapsed navigation should keep tooltip support
- drawers should use a clickable overlay when they open above the workspace
- reduced navigation must keep icons optically centered
- search bars should support a stronger focus treatment than neutral inputs

### Administration UI rules

The administration UI is optimized for control-plane work.

Its expected characteristics are:

- strong side navigation
- frequent lists and tables
- dashboard homepage built from modular cards
- KPI and system-health cards
- higher information density than the client UI
- emphasis on system state, policies, quarantine, routing, and operations

Navigation must be grouped by functional themes rather than isolated technical tools, for example:

- Dashboard
- Tenants and Domains
- Accounts and Mailboxes
- Mail Flow and Quarantine
- Security and Policies
- Audit and Compliance
- Storage and Lifecycle
- Protocols and Client Access
- System Operations

The administration shell should support:

- a simplified view for frequent users
- an advanced view for deeper operations

Switching between them may change density and navigation depth, but must not break the shell.

The default administration management pattern remains:

- full-width list
- primary `New` or `Create` action in the list header
- right-side drawer for creation, details, and contextual actions

Drawers should remain deep-linkable so a URL can reopen a specific record or form.

Administration dashboard cards should cover at minimum:

- global system state
- alerts and incidents
- critical service health
- inbound and outbound summaries
- quarantine
- storage capacity
- frequent operator shortcuts

Cards should be reorderable.

### Client UI rules

The client UI is optimized for mailbox and collaboration comfort.

Its expected characteristics are:

- lower density than administration
- reading comfort first
- three-pane and workspace layouts where relevant
- softer hierarchy while keeping the shared visual language
- frequent interactions around list, reading, composition, calendar, contacts, and tasks

Expected client patterns include:

- `ClientShell`
- `MailThreePaneLayout`
- `ReadingPane`
- `ComposeDrawer`
- `CalendarWorkspace`
- `ContactsWorkspace`
- `TaskWorkspace`

### Visual direction

The visual system should remain restrained, readable, and operational.

Base rules:

- light or neutral background
- soft separation between navigation, toolbars, and content
- lightly rounded corners
- subtle but present depth
- readable typography with clear hierarchy
- no harsh black text on bright white everywhere; prefer softened contrast for metadata and secondary information

Premium treatments may include:

- `bg-white/70`
- `backdrop-blur`
- luminous borders
- subtle inset `ring`
- hover lift
- restrained scale feedback
- polished contextual surfaces such as menus and drawers

Semantic colors:

- green for success
- red for alert or error
- blue for informational state
- orange or amber for warning

### Accessibility and responsive behavior

The interface must:

- remain keyboard-usable
- keep sufficient contrast
- preserve clear action targets
- adapt to desktop, laptop, tablet, and mobile sizes
- keep the same component logic across layouts

On narrow screens:

- the sidebar may become off-canvas
- the top bar remains present
- cards and content grids reflow
- drawers may take more width, but keep the same role

### Implementation conventions

- keep utility composition inside primitives and reusable wrappers where possible
- avoid long one-off utility strings directly inside domain features
- keep business components focused on behavior and orchestration
- prefer reusable premium effects inside primitives: `glass panel`, `focus ring`, `hover lift`, `status badge`, `collapsed tooltip`
- keep `web/admin` and `web/client` visually aligned, even when density differs
- when migrating legacy views, prefer moving repeated controls first (`Button`, `Input`, `Select`, `Textarea`, `Badge`, `Card`, `Drawer`, `Tabs`) before attempting a full feature rewrite

### Reference demos

The following demos are the current visual reference:

- `docs/architecture/admin-web-design-preview-tailwind.html`
- `docs/architecture/client-web-design-preview-tailwind.html`

They are used to validate shell, density, visual tokens, and interaction patterns before product integration.

