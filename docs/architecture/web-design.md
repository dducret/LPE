# Web Design | Design web

## English

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

`Tailwind CSS` is the normalization base for all `LPE` web interfaces.

This does not mean screens should be built as uncontrolled one-off utility strings. The intended model is:

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

The recommended long-term structure is:

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

### Reference demos

The following demos are the current visual reference:

- `docs/architecture/admin-web-design-preview-tailwind.html`
- `docs/architecture/client-web-design-preview-tailwind.html`

They are used to validate shell, density, visual tokens, and interaction patterns before product integration.

## Francais

### Objectif

Ce document definit la direction commune du design web pour `LPE`.

Il fusionne l'ancienne documentation UI d'administration et l'ancienne documentation du design system `Tailwind CSS` dans une seule reference d'architecture pour :

- `web/admin`
- `web/client`

L'objectif est de conserver un langage web coherent entre les produits tout en autorisant des differences de densite et de workflow entre l'administration et l'experience utilisateur finale.

### Quand utiliser ce document

Ce document doit etre traite comme la reference d'architecture pour toute tache qui modifie :

- le layout
- la navigation
- le style des composants
- le responsive
- les drawers, dialogs, overlays et toolbars
- les tokens partages ou les conventions `Tailwind`
- les patterns d'interaction cote administration ou client

### Technologie et base de normalisation

`Tailwind CSS` est la base de normalisation de toutes les interfaces web `LPE`.

Cela ne signifie pas que les ecrans doivent etre construits comme des chaines de classes utilitaires sans controle. Le modele vise est :

- un theme partage
- des primitives reutilisables
- des variantes documentees
- des composants metier construits au-dessus de ces primitives

La cible visuelle est un rendu `SaaS premium` contemporain :

- surfaces claires semi-transparentes
- `glassmorphism` modere
- `backdrop blur`
- reflets subtils sur les aretes
- badges semantiques plus riches
- micro-interactions discretes
- mouvements organiques

### Regles du design system partage

Le design system partage doit fournir :

- des tokens centralises pour couleurs, espacements, rayons, ombres, focus et etats semantiques
- des primitives communes pour `Button`, `Input`, `Textarea`, `Select`, `Badge`, `Card`, `Drawer`, `Dialog`, `Tabs`, `Table`, `Toolbar`, `Sidebar`, `EmptyState`, `Avatar`, `Dropdown` et `Toast`
- des variantes reutilisables plutot que des longues chaines de classes repetees
- un helper de composition type `cn()` et une logique de variantes reutilisable

Le theme partage doit couvrir au minimum :

- couleurs neutres
- couleurs semantiques
- echelle typographique
- echelle d'espacement
- rayons
- presets d'ombres
- breakpoints
- styles de focus
- niveaux de transparence des surfaces
- niveaux de `backdrop blur`
- traitements `ring` internes pour les surfaces premium
- courbes de transition standards, y compris une courbe organique type `spring`

Exemples de tokens semantiques a normaliser :

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

Exemples de variantes a normaliser :

- `Button`: `primary`, `secondary`, `ghost`, `danger`
- `Badge`: `info`, `success`, `warning`, `danger`, `neutral`
- `Card`: `default`, `soft`, `elevated`, `interactive`, `premium`
- `Drawer`: `default`, `wide`, `form`, `details`
- `Table`: `compact`, `default`, `comfortable`
- `Panel`: `default`, `glass`
- `Search`: `default`, `focused`
- `Sidebar`: `default`, `collapsed`

### Structure cible du depot

La structure recommandee a terme est :

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

### Regles de shell partage

`web/admin` et `web/client` doivent donner l'impression d'appartenir a la meme famille produit.

Attendus partages du shell :

- barre laterale gauche fixe comme structure principale de navigation
- sidebar retractable sur desktop
- barre d'outils superieure pour recherche, contexte, aide et profil
- surfaces claires premium avec profondeur retenue
- drawers lateraux droits pour les actions contextuelles quand c'est pertinent

Regles d'implementation :

- la retractation de la sidebar doit privilegier une variation de largeur du panneau lateral plutot qu'une reconfiguration brutale de toute la grille
- la navigation reduite doit conserver des infobulles
- les drawers doivent utiliser un overlay cliquable lorsqu'ils s'ouvrent au-dessus du workspace
- la navigation reduite doit garder un centrage optique propre des icones
- les barres de recherche doivent avoir un traitement de focus plus fort qu'un champ neutre

### Regles UI administration

L'UI d'administration est optimisee pour le plan de controle.

Ses caracteristiques attendues sont :

- navigation laterale forte
- listes et tableaux frequents
- page d'accueil sous forme de dashboard a cartes modulaires
- cartes KPI et etat systeme
- densite d'information plus elevee que l'UI client
- accent sur l'etat systeme, les policies, la quarantaine, le routage et les operations

La navigation doit etre groupee par thematiques fonctionnelles plutot que par outils techniques isoles, par exemple :

- Dashboard
- Tenants and Domains
- Accounts and Mailboxes
- Mail Flow and Quarantine
- Security and Policies
- Audit and Compliance
- Storage and Lifecycle
- Protocols and Client Access
- System Operations

Le shell d'administration doit supporter :

- une vue simplifiee pour les utilisateurs frequents
- une vue avancee pour les operations plus profondes

Le passage de l'une a l'autre peut changer la densite et la profondeur de navigation, mais ne doit pas casser le shell.

Le pattern de gestion d'administration retenu reste :

- liste pleine largeur
- action primaire `New` ou `Create` dans l'entete de liste
- drawer lateral droit pour creation, details et actions contextuelles

Les drawers doivent rester deep-linkable pour qu'une URL puisse rouvrir une fiche ou un formulaire specifique.

Les cartes du dashboard d'administration doivent couvrir au minimum :

- etat global du systeme
- alertes et incidents
- sante des services critiques
- resumes entrants et sortants
- quarantaine
- capacite de stockage
- raccourcis operateur frequents

Les cartes doivent etre reordonnables.

### Regles UI client

L'UI client est optimisee pour le confort mailbox et collaboration.

Ses caracteristiques attendues sont :

- densite plus faible que l'administration
- confort de lecture prioritaire
- layouts en trois panneaux et workspaces quand c'est pertinent
- hierarchie plus douce tout en gardant le langage visuel partage
- interactions frequentes autour de la liste, de la lecture, de la composition, du calendrier, des contacts et des taches

Patterns client attendus :

- `ClientShell`
- `MailThreePaneLayout`
- `ReadingPane`
- `ComposeDrawer`
- `CalendarWorkspace`
- `ContactsWorkspace`
- `TaskWorkspace`

### Direction visuelle

Le systeme visuel doit rester sobre, lisible et operationnel.

Regles de base :

- fond clair ou neutre
- separation douce entre navigation, toolbars et contenu
- coins legerement arrondis
- profondeur presente mais discrete
- typographie lisible avec hierarchie claire
- eviter un contraste noir pur sur blanc pur partout ; preferer un contraste adouci pour les metadonnees et l'information secondaire

Les traitements premium peuvent inclure :

- `bg-white/70`
- `backdrop-blur`
- bordures lumineuses
- `ring` interne discret
- hover lift
- feedback de scale retenu
- surfaces contextuelles plus polies pour menus et drawers

Couleurs semantiques :

- vert pour succes
- rouge pour alerte ou erreur
- bleu pour information
- orange ou ambre pour avertissement

### Accessibilite et responsive

L'interface doit :

- rester utilisable au clavier
- conserver un contraste suffisant
- garder des cibles d'action claires
- s'adapter au desktop, laptop, tablette et mobile
- conserver la meme logique de composants selon le layout

Sur ecrans etroits :

- la sidebar peut devenir off-canvas
- la top bar reste presente
- les cartes et grilles de contenu se reflow
- les drawers peuvent prendre plus de largeur mais gardent le meme role

### Conventions d'implementation

- garder la composition des utilitaires dans des primitives et wrappers reutilisables quand c'est possible
- eviter les longues chaines de classes one-off directement dans les features metier
- garder les composants metier centres sur le comportement et l'orchestration
- preferer des primitives qui encapsulent aussi les effets premium recurrents : `glass panel`, `focus ring`, `hover lift`, `status badge`, `collapsed tooltip`
- garder `web/admin` et `web/client` visuellement alignes, meme si leur densite differe

### Demos de reference

Les demos suivantes sont la reference visuelle actuelle :

- `docs/architecture/admin-web-design-preview-tailwind.html`
- `docs/architecture/client-web-design-preview-tailwind.html`

Elles servent a valider le shell, la densite, les tokens visuels et les patterns d'interaction avant integration produit.
