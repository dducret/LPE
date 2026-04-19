# Admin Web Design | Design web d'administration

## Francais

### Objectif

Ce document fixe la direction UI/UX de l'interface d'administration web `LPE` et `LPE-CT`.

L'objectif est une interface moderne, epuree, coherente, et fortement unifiee, capable de servir a la fois les usages frequents et les operations plus profondes sans multiplier les patterns de navigation.

### Principes de design

- structure a panneaux avec barre laterale verticale fixe, retractable, et barre d'outils superieure
- tableau de bord central base sur des cartes modulaires
- hierarchie visuelle claire
- coins legerement arrondis
- palette sobre avec semantique explicite
- meme logique de composants sur desktop, laptop, tablette et mobile

### Technologie UI de reference

`Tailwind CSS` est retenu comme base de normalisation pour les interfaces web de `LPE`.

Cette decision s'applique a la fois a:

- `web/admin`
- `web/client`

L'objectif n'est pas d'encourager un assemblage libre de classes utilitaires ecran par ecran, mais de construire un design system partage au-dessus de `Tailwind`.

Les regles de base sont:

- theme partage avec tokens centralises pour couleurs, espacements, radius, ombres et etats semantiques
- composants reutilisables et variantes documentees pour `button`, `input`, `card`, `badge`, `drawer`, `table`, `toolbar`, `sidebar` et `empty state`
- convergence visuelle entre administration et client web, avec un langage commun mais des densites adaptees au contexte
- pas de logique de style parallele ou incoherente entre `web/admin` et `web/client`
- les composants metier doivent encapsuler les combinaisons utilitaires au lieu de dupliquer de longues chaines de classes partout
- la direction visuelle cible est un rendu `SaaS premium` contemporain: surfaces claires semi-transparentes, `backdrop blur`, reflets subtils, badges plus riches, micro-interactions discretes et transitions organiques

### Structure generale

L'interface d'administration suit le shell suivant:

- une barre laterale gauche fixe pour la navigation principale
- une top bar pour la recherche globale, le contexte courant, les notifications et le profil
- une zone de contenu principale
- des drawers lateraux pour creation, edition, details et actions contextuelles

La barre laterale doit etre retractable, mais le modele de navigation doit rester stable entre mode ouvert et mode reduit.

### Navigation

La navigation ne doit pas etre structuree par outils techniques isoles.

Elle doit etre regroupee par thematiques fonctionnelles, par exemple:

- Dashboard
- Tenants and Domains
- Accounts and Mailboxes
- Mail Flow and Quarantine
- Security and Policies
- Audit and Compliance
- Storage and Lifecycle
- Protocols and Client Access
- System Operations

L'interface doit proposer deux niveaux de profondeur:

- une vue simplifiee pour les utilisateurs frequents et les taches recurrentes
- une vue avancee pour les reglages profonds et l'exploitation detaillee

Le passage entre ces deux vues doit changer la densite et la profondeur des options, sans casser la structure globale.

### Tableau de bord

La page d'accueil doit etre un tableau de bord central a base de cartes.

Les cartes doivent couvrir au minimum:

- etat global du systeme
- alertes et incidents
- sante des services critiques
- resume transport entrant et sortant
- quarantaine
- capacite stockage
- raccourcis vers les taches frequentes

Les cartes doivent etre reordonnables afin d'adapter l'ecran d'accueil au profil operateur.

### Design system

Le design system doit rester sobre et operationnel.

Les regles de base sont:

- fond clair ou neutre
- separation visuelle douce entre navigation, top bar et contenu
- cartes blanches ou legerement teintees sur fond structure
- coins legerement arrondis
- ombres discretes
- typographie lisible avec hierarchie nette
- surfaces premium possibles via `glassmorphism` modere: `bg-white/70`, `backdrop-blur`, bordures lumineuses et `ring` interne discret
- micro-interactions courtes sur les cartes, lignes, actions rapides et drawers

Couleurs semantiques:

- vert pour succes
- rouge pour alerte ou erreur
- bleu pour information
- orange ou ambre pour avertissement

### Accessibilite et responsive

L'interface doit:

- rester utilisable au clavier
- conserver des contrastes suffisants
- garder des zones d'action claires
- s'adapter a plusieurs tailles d'ecran
- conserver les memes composants et la meme logique de navigation selon le format

Sur ecrans etroits:

- la barre laterale peut devenir off-canvas
- la top bar reste presente
- les cartes se reflow en une seule colonne ou une grille reduite
- les drawers doivent occuper une largeur adaptee mais garder le meme usage

### Pattern de gestion

Le pattern par defaut deja retenu par `LPE` reste applicable:

- liste pleine largeur
- action primaire `New` ou `Create` dans l'entete
- drawer lateral droit pour creation, details et actions contextuelles

Les drawers doivent rester deep-linkable pour qu'une URL puisse rouvrir une fiche ou un formulaire specifique.

En implementation:

- la sidebar retractable doit preferer une variation de largeur du panneau lateral plutot qu'une reconfiguration brutale de la grille entiere
- les elements de navigation reduits doivent rester accompagnés d'infobulles en mode collapsed
- les drawers lateraux doivent etre accompagnes d'un overlay cliquable pour fermer l'action contextuelle proprement

### Wireframe HTML/CSS de reference

```html
<div class="app-shell">
  <aside class="sidebar">
    <div class="brand">LPE Admin</div>
    <nav class="nav-groups">
      <a class="nav-item active">Dashboard</a>
      <a class="nav-item">Tenants & Domains</a>
      <a class="nav-item">Accounts & Mailboxes</a>
      <a class="nav-item">Mail Flow & Quarantine</a>
      <a class="nav-item">Security & Policies</a>
      <a class="nav-item">Audit & Compliance</a>
      <a class="nav-item">Storage & Lifecycle</a>
      <a class="nav-item">Protocols & Access</a>
      <a class="nav-item">System Operations</a>
    </nav>
    <div class="sidebar-footer">
      <button>Simple View</button>
      <button>Advanced View</button>
    </div>
  </aside>

  <div class="workspace">
    <header class="topbar">
      <div class="topbar-left">
        <button class="sidebar-toggle">☰</button>
        <input class="global-search" placeholder="Search domains, accounts, queues..." />
      </div>
      <div class="topbar-right">
        <button class="icon-button">Alerts</button>
        <button class="icon-button">Help</button>
        <div class="profile-chip">admin@example.test</div>
      </div>
    </header>

    <main class="content">
      <section class="hero-row">
        <div class="hero-card success">System Healthy</div>
        <div class="hero-card info">12 Deferred Messages</div>
        <div class="hero-card warning">3 Quarantine Releases Pending</div>
        <div class="hero-card danger">1 Node Requires Attention</div>
      </section>

      <section class="card-grid">
        <article class="card">Inbound Flow Summary</article>
        <article class="card">Outbound Relay Summary</article>
        <article class="card">Queue Health</article>
        <article class="card">Storage Capacity</article>
        <article class="card">Recent Audit Events</article>
        <article class="card">Quick Actions</article>
      </section>

      <section class="list-layout">
        <div class="list-header">
          <h2>Domains</h2>
          <button class="primary">Create</button>
        </div>
        <div class="list-panel">Full-width administration list</div>
      </section>
    </main>
  </div>
</div>
```

```css
:root {
  --bg: #f4f6f8;
  --surface: #ffffff;
  --surface-muted: #eef2f6;
  --border: #d9e0e7;
  --text: #18212b;
  --text-soft: #5d6b79;
  --success: #1f9d63;
  --danger: #cf3d3d;
  --info: #2f6fed;
  --warning: #c98918;
  --radius: 14px;
  --shadow: 0 10px 30px rgba(20, 32, 48, 0.08);
}

body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font: 14px/1.4 "Segoe UI", "Inter", sans-serif;
}

.app-shell {
  display: grid;
  grid-template-columns: 280px 1fr;
  min-height: 100vh;
}

.sidebar {
  background: #111822;
  color: #f8fbff;
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.workspace {
  display: grid;
  grid-template-rows: 72px 1fr;
}

.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 24px;
  background: rgba(255, 255, 255, 0.88);
  border-bottom: 1px solid var(--border);
  backdrop-filter: blur(10px);
}

.content {
  padding: 24px;
  display: grid;
  gap: 24px;
}

.hero-row,
.card-grid {
  display: grid;
  gap: 16px;
}

.hero-row {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.card-grid {
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
}

.card,
.hero-card,
.list-panel {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 20px;
}

.success { border-left: 4px solid var(--success); }
.danger  { border-left: 4px solid var(--danger); }
.info    { border-left: 4px solid var(--info); }
.warning { border-left: 4px solid var(--warning); }

@media (max-width: 1100px) {
  .app-shell {
    grid-template-columns: 88px 1fr;
  }
}

@media (max-width: 760px) {
  .app-shell {
    grid-template-columns: 1fr;
  }

  .sidebar {
    display: none;
  }

  .hero-row {
    grid-template-columns: 1fr;
  }
}
```

## English

### Goal

This document defines the UI/UX direction for the `LPE` and `LPE-CT` web administration interfaces.

The goal is a modern, clean, highly unified administration experience that supports both frequent actions and deeper operational workflows without multiplying navigation models.

### Design principles

- panel-based structure with a fixed collapsible vertical sidebar and top toolbar
- dashboard homepage built from modular cards
- clear visual hierarchy
- lightly rounded corners
- restrained palette with semantic status colors
- same component logic across desktop, laptop, tablet, and mobile

### Reference UI technology

`Tailwind CSS` is the selected normalization base for the `LPE` web interfaces.

This decision applies to both:

- `web/admin`
- `web/client`

The goal is not to encourage uncontrolled utility-class composition screen by screen, but to build a shared design system on top of `Tailwind`.

The base rules are:

- shared theme with centralized tokens for color, spacing, radius, shadows, and semantic states
- reusable components and documented variants for `button`, `input`, `card`, `badge`, `drawer`, `table`, `toolbar`, `sidebar`, and `empty state`
- visual convergence between the administration UI and the web client, with a shared language but density adapted to each context
- no parallel or inconsistent styling logic between `web/admin` and `web/client`
- product components should encapsulate utility combinations instead of duplicating long class strings everywhere
- the target visual direction is a contemporary `premium SaaS` rendering: light semi-transparent surfaces, `backdrop blur`, subtle highlight edges, richer badges, restrained micro-interactions, and organic transitions

### General structure

The administration interface uses the following shell:

- fixed left sidebar for primary navigation
- top bar for global search, current context, notifications, and profile
- main content area
- side drawers for creation, editing, details, and contextual actions

The sidebar may collapse, but the navigation model must remain stable between expanded and reduced states.

### Navigation

Navigation must not be grouped around isolated technical tools.

It should be grouped by functional themes, for example:

- Dashboard
- Tenants and Domains
- Accounts and Mailboxes
- Mail Flow and Quarantine
- Security and Policies
- Audit and Compliance
- Storage and Lifecycle
- Protocols and Client Access
- System Operations

The interface should support two depth levels:

- a simplified view for frequent users and recurrent tasks
- an advanced view for deeper configuration and operational detail

Switching between those views should change density and depth, without breaking the global shell.

### Dashboard

The home page should be a card-based dashboard.

At minimum, cards should cover:

- global system state
- alerts and incidents
- critical service health
- inbound and outbound transport summaries
- quarantine
- storage capacity
- shortcuts to common actions

Cards should be reorderable so the homepage can adapt to the operator profile.

### Design system

The design system should remain restrained and operational.

Base rules:

- light or neutral background
- soft separation between navigation, top bar, and content
- white or lightly tinted cards on a structured background
- lightly rounded corners
- subtle shadows
- readable typography with clear hierarchy
- premium surfaces may use moderated `glassmorphism`: `bg-white/70`, `backdrop blur`, luminous borders, and a soft inset `ring`
- interactions should feel reactive without becoming noisy: short hover elevation, subtle scale, focus emphasis, and restrained motion

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
- adapt to different screen sizes
- retain the same components and navigation logic across layouts

On narrow screens:

- the sidebar may become off-canvas
- the top bar remains present
- cards reflow into a reduced grid or single column
- drawers should expand appropriately while keeping the same role

### Management pattern

The default `LPE` management pattern remains valid:

- full-width list
- primary `New` or `Create` action in the header
- right-side drawer for creation, details, and contextual actions

Drawers must remain deep-linkable so a URL can reopen a specific form or record.

In implementation:

- the collapsible sidebar should prefer a width change on the side panel rather than abrupt reconfiguration of the whole grid
- reduced navigation items should keep tooltip support in collapsed mode
- side drawers should include a clickable overlay so contextual actions can be dismissed cleanly

### Reference HTML/CSS wireframe

The HTML/CSS structure shown in the French section is the baseline wireframe deliverable for this administration architecture.
