# La Poste Electronique

## Francais

`LPE` est une plateforme de messagerie et de collaboration moderne, multiplateforme, ecrite majoritairement en Rust.

### Principes initiaux

- code projet sous licence `Apache-2.0`
- dependances `MIT` autorisees uniquement si aucune alternative `Apache-2.0` raisonnable n'existe
- `PostgreSQL` comme stockage primaire de metadonnees
- `JMAP` comme axe principal du produit moderne
- `IMAP` comme couche de compatibilite mailbox
- transport `SMTP` entrant et sortant porte par le centre de tri `LPE-CT`
- compatibilite client native visee via `IMAP` et compatibilite Outlook native critique via `ActiveSync`, `EWS` ou couche equivalente, sans casser la coherence des messages envoyes dans `LPE`
- architecture preparee pour une IA locale future sans sortie de donnees hors serveur

### Structure

- `crates/` services et bibliotheques Rust
- `web/admin` back office React/TypeScript
- `web/client` client web type Outlook Web avec shell mail, liste de messages et panneau de lecture
- `LPE-CT/` centre de tri DMZ separe avec sa propre documentation et ses scripts Debian
- `docs/architecture` decisions techniques initiales
- `docs/licensing` politique de licences et garde-fous CI
- `installation` scripts et documentation de deploiement

### Demarrage

Le squelette actuel permet de compiler les crates Rust du workspace.

```powershell
cargo check
```

Sur `Debian Trixie`, les scripts d'installation deploient aussi une console d'administration statique via `nginx`, avec reverse proxy `/api/` vers le service Rust local.

Pour un serveur de tri distinct en `DMZ`, le sous-repertoire `LPE-CT/` fournit un composant autonome avec:

- une API Rust de management
- une interface statique dediee
- des scripts `Debian Trixie` d'installation et de mise a jour dans `LPE-CT/installation/debian-trixie`

La console d'administration actuelle couvre deja une V1 de pilotage du plan de controle:

- page serveur avec etat, politiques, domaines et administrateurs
- page domaine orientee gestion des comptes, alias et delegations
- actions d'import et d'export `PST` au niveau des boites aux lettres depuis la page domaine
- page antispam avec moteur, regles et quarantaine
- page audit et compliance avec journal et recherche de trace email
- page operations pour protocoles et stockage

Cette console est maintenant persistante dans `PostgreSQL` via `lpe-storage` et les migrations SQL du projet.

Le protocole moderne principal reste `JMAP`, mais `LPE` doit aussi rester compatible avec des clients natifs. Cela vise en particulier les usages de type application Mail sur iPhone et les clients Outlook. Le support Outlook natif est critique pour l'adoption et ne doit pas etre reduit a `IMAP` + `SMTP` + autodiscover. Dans tous les cas, un message envoye depuis un client externe doit etre enregistre dans `LPE` et rester visible dans la vue `Sent` de maniere coherente sur tous les acces.

### Axe IA locale

`LPE` prepare des projections documentaires canoniques, des chunks et des annotations qui serviront plus tard a une integration LLM locale type Gemma, sans rendre le moteur IA dependance du coeur metier.

### Formats documentaires v1

Les pieces jointes suivantes sont prevues pour l'indexation texte en v1:

- `PDF` via `pdf_oxide`
- `DOCX` via `docx-lite`
- `ODT` via un extracteur ODF minimal focalise texte

### Interfaces multilingues

Les interfaces web supportent en v1:

- anglais par defaut
- francais
- allemand
- italien
- espagnol

## English

`LPE` is a modern cross-platform mail and collaboration platform written primarily in Rust.

### Initial principles

- project code is licensed under `Apache-2.0`
- `MIT` dependencies are allowed only when no reasonable `Apache-2.0` alternative exists
- `PostgreSQL` is the primary metadata store
- `JMAP` is the main protocol axis for the modern product
- `IMAP` is a mailbox compatibility layer
- inbound and outbound `SMTP` transport is handled by the `LPE-CT` sorting center
- native client compatibility is a target through `IMAP`, and native Outlook compatibility is adoption-critical through `ActiveSync`, `EWS`, or an equivalent layer, without breaking sent-message consistency inside `LPE`
- the architecture is prepared for future local AI without data leaving the server

### Structure

- `crates/` Rust services and libraries
- `web/admin` React/TypeScript back office
- `web/client` Outlook Web style client with a mail shell, message list, and reading pane
- `LPE-CT/` separate DMZ sorting center with its own documentation and Debian scripts
- `docs/architecture` initial technical decisions
- `docs/licensing` license policy and CI guardrails
- `installation` deployment scripts and documentation

### Getting started

The current skeleton compiles the Rust workspace crates.

```powershell
cargo check
```

On `Debian Trixie`, the installation scripts also deploy a static administration console through `nginx`, with `/api/` reverse-proxied to the local Rust service.

For a separate sorting server placed in a `DMZ`, the `LPE-CT/` subdirectory provides an autonomous component with:

- a Rust management API
- a dedicated static interface
- `Debian Trixie` install and update scripts in `LPE-CT/installation/debian-trixie`

The current administration console already exposes a first control-plane V1:

- server page with status, policies, domains, and administrators
- domain-oriented page for accounts, aliases, and delegated administration
- mailbox-level `PST` import and export actions from the domain page
- anti-spam page with engine, rules, and quarantine
- audit and compliance page with journal and email trace search
- operations page for protocols and storage

This console is now persisted in `PostgreSQL` through `lpe-storage` and the project's SQL migrations.

The main modern protocol remains `JMAP`, but `LPE` must also stay compatible with native clients. This especially targets use cases such as the iPhone Mail application and Outlook clients. Native Outlook support is critical for adoption and must not be reduced to `IMAP` + `SMTP` + autodiscover. In every case, a message sent from an external client must be recorded in `LPE` and remain visible in the authoritative `Sent` view across access paths.

### Local AI direction

`LPE` prepares canonical document projections, chunks, and annotations so a future local LLM such as Gemma can be integrated without making the AI engine a core product dependency.

### v1 document formats

The following attachment formats are planned for text indexing in v1:

- `PDF` via `pdf_oxide`
- `DOCX` via `docx-lite`
- `ODT` via a minimal text-focused ODF extractor

### Multilingual interfaces

The web interfaces support the following languages in v1:

- English by default
- French
- German
- Italian
- Spanish
