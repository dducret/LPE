# Web Tailwind Design System | Design system web Tailwind

## Francais

### Objectif

Ce document definit la structure cible du design system `Tailwind CSS` partage entre:

- `web/admin`
- `web/client`

Le but est d'eviter deux stacks UI distinctes et de construire une base commune:

- meme theme
- memes primitives UI
- memes conventions de variantes
- langage visuel coherent
- densite et workflows adaptes au contexte administration ou client

### Principe general

`Tailwind CSS` est la couche de normalisation.

Le design system ne doit pas vivre dans de longues chaines de classes ecrites partout dans les vues. Il doit etre organise autour de:

- tokens de theme
- primitives reutilisables
- composants composes par domaine
- patterns d'ecrans documentes

### Structure recommandee du repo

Base cible recommandee:

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
```

### Theme partage

Le theme doit etre centralise dans un preset partage ou un module commun.

Il doit definir au minimum:

- couleurs neutres
- couleurs semantiques
- espacements
- rayons
- ombres
- tailles de police
- breakpoints
- styles focus

Exemple de tokens a normaliser:

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

Le theme partage doit aussi couvrir:

- niveaux de transparence de surface
- intensite de `backdrop blur`
- `ring` internes pour les surfaces premium
- ombres courtes et ombres profondes
- courbes de transition standards, y compris une courbe organique type `spring`

### Primitives UI a partager

Les primitives suivantes doivent etre communes a `web/admin` et `web/client`:

- `Button`
- `Input`
- `Textarea`
- `Select`
- `Badge`
- `Card`
- `Drawer`
- `Dialog`
- `Tabs`
- `Table`
- `Toolbar`
- `Sidebar`
- `EmptyState`
- `Avatar`
- `Dropdown`
- `Toast`

Chaque primitive doit definir clairement:

- ses variantes
- ses tailles
- ses etats
- ses contraintes d'accessibilite

### Variantes recommandees

Exemples de variantes a normaliser:

- `Button`: `primary`, `secondary`, `ghost`, `danger`
- `Badge`: `info`, `success`, `warning`, `danger`, `neutral`
- `Card`: `default`, `soft`, `elevated`, `interactive`
- `Drawer`: `default`, `wide`, `form`, `details`
- `Table`: `compact`, `default`, `comfortable`

Des variantes premium doivent etre prevues au niveau systeme:

- `Card.premium`
- `Panel.glass`
- `Badge.glass`
- `Search.focused`
- `Sidebar.collapsed`

### Differences admin vs client

Le langage visuel doit converger, mais les produits n'ont pas la meme densite.

Pour `web/admin`:

- navigation laterale forte
- listes et tableaux frequents
- cartes KPI
- drawers d'administration
- densite plus elevee
- mise en avant des etats systeme et actions rapides

Pour `web/client`:

- priorite au confort de lecture
- shell de messagerie et collaboration
- hierarchie plus douce
- densite plus faible
- interactions frequentes autour de liste, lecture, composition, calendrier et contacts

### Patterns de layout

Patterns cibles cote administration:

- `AdminShell`
- `DashboardGrid`
- `FullWidthList + RightDrawer`
- `DetailDrawer`
- `HealthCard`
- `Toolbar + Filters + Table`

Patterns cibles cote client:

- `ClientShell`
- `MailThreePaneLayout`
- `ReadingPane`
- `ComposeDrawer`
- `CalendarWorkspace`
- `ContactsWorkspace`
- `TaskWorkspace`

### Conventions d'implementation

- utiliser un helper `cn()` pour composer proprement les classes
- encapsuler les variantes dans une logique reusable plutot que repeter les classes
- eviter les classes utilitaires tres longues directement dans les features metier
- reserver les primitives aux styles generiques et les composants metier a l'orchestration fonctionnelle
- garder les ecrans lisibles et faiblement couples au theme
- privilegier des primitives qui encapsulent aussi les effets premium recurrents: `glass panel`, `focus ring`, `hover lift`, `status badge`, `collapsed tooltip`

### Demostrations de reference

Demonstration serveur / administration:

- `docs/architecture/admin-web-design-preview-tailwind.html`

Demonstration client:

- `docs/architecture/client-web-design-preview-tailwind.html`

Ces demos servent a valider le shell, la densite, les tokens visuels et les patterns de navigation avant integration produit.

## English

### Goal

This document defines the target `Tailwind CSS` design-system structure shared by:

- `web/admin`
- `web/client`

The goal is to avoid two separate UI stacks and instead build a common base with:

- one theme
- shared UI primitives
- shared variant conventions
- a coherent visual language
- context-specific density and workflows for administration and client use

### General principle

`Tailwind CSS` is the normalization layer.

The design system must not live as long class strings spread across every screen. It should be organized around:

- theme tokens
- reusable primitives
- domain-composed components
- documented screen patterns

### Recommended repository structure

Recommended target base:

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
```

### Shared theme

The theme must be centralized in a shared preset or common module.

At minimum it must define:

- neutral colors
- semantic colors
- spacing
- radii
- shadows
- font sizes
- breakpoints
- focus styles

Example tokens to normalize:

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

The shared theme should also cover:

- surface transparency levels
- `backdrop blur` intensity
- inset `ring` treatments for premium surfaces
- shallow and deep shadow presets
- standard transition curves, including an organic `spring`-like curve

### Shared UI primitives

The following primitives should be common to both `web/admin` and `web/client`:

- `Button`
- `Input`
- `Textarea`
- `Select`
- `Badge`
- `Card`
- `Drawer`
- `Dialog`
- `Tabs`
- `Table`
- `Toolbar`
- `Sidebar`
- `EmptyState`
- `Avatar`
- `Dropdown`
- `Toast`

Each primitive should clearly define:

- variants
- sizes
- states
- accessibility constraints

### Recommended variants

Examples of variants to normalize:

- `Button`: `primary`, `secondary`, `ghost`, `danger`
- `Badge`: `info`, `success`, `warning`, `danger`, `neutral`
- `Card`: `default`, `soft`, `elevated`, `interactive`
- `Drawer`: `default`, `wide`, `form`, `details`
- `Table`: `compact`, `default`, `comfortable`

Premium variants should also be planned at the system level:

- `Card.premium`
- `Panel.glass`
- `Badge.glass`
- `Search.focused`
- `Sidebar.collapsed`

### Admin vs client differences

The visual language should converge, but the products do not have the same density.

For `web/admin`:

- strong side navigation
- frequent lists and tables
- KPI cards
- administration drawers
- higher density
- emphasis on system states and quick actions

For `web/client`:

- reading comfort first
- mail and collaboration shell
- softer hierarchy
- lower density
- frequent interactions around list, reading, composition, calendar, and contacts

### Layout patterns

Target administration patterns:

- `AdminShell`
- `DashboardGrid`
- `FullWidthList + RightDrawer`
- `DetailDrawer`
- `HealthCard`
- `Toolbar + Filters + Table`

Target client patterns:

- `ClientShell`
- `MailThreePaneLayout`
- `ReadingPane`
- `ComposeDrawer`
- `CalendarWorkspace`
- `ContactsWorkspace`
- `TaskWorkspace`

### Implementation conventions

- use a `cn()` helper for clean class composition
- encapsulate variants in reusable logic rather than repeating class lists
- avoid very long utility strings directly inside domain features
- reserve primitives for generic styling and domain components for functional orchestration
- keep screens readable and loosely coupled to theme internals
- prefer primitives that also encapsulate recurring premium effects: `glass panel`, `focus ring`, `hover lift`, `status badge`, and `collapsed tooltip`

### Reference demos

Server / administration demo:

- `docs/architecture/admin-web-design-preview-tailwind.html`

Client demo:

- `docs/architecture/client-web-design-preview-tailwind.html`

Those demos are used to validate shell, density, visual tokens, and navigation patterns before product integration.
