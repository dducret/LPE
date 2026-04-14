# La Poste ELectronique

## Francais

`LPE` est une plateforme de messagerie et de collaboration moderne, multiplateforme, ecrite majoritairement en Rust.

### Principes initiaux

- code projet sous licence `Apache-2.0`
- dependances `MIT` autorisees uniquement si aucune alternative `Apache-2.0` raisonnable n'existe
- `PostgreSQL` comme stockage primaire de metadonnees
- `JMAP` comme axe principal du produit moderne
- `IMAP` et `SMTP` comme couches de compatibilite
- architecture preparee pour une IA locale future sans sortie de donnees hors serveur

### Structure

- `crates/` services et bibliotheques Rust
- `web/admin` back office React/TypeScript
- `web/client` client web type Outlook Web
- `docs/architecture` decisions techniques initiales
- `docs/licensing` politique de licences et garde-fous CI
- `installation` scripts et documentation de deploiement

### Demarrage

Le squelette actuel permet de compiler les crates Rust du workspace.

```powershell
cargo check
```

Sur `Debian Trixie`, les scripts d'installation deploient aussi une premiere console d'administration statique via `nginx`, avec reverse proxy `/api/` vers le service Rust local.

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
- `IMAP` and `SMTP` are compatibility layers
- the architecture is prepared for future local AI without data leaving the server

### Structure

- `crates/` Rust services and libraries
- `web/admin` React/TypeScript back office
- `web/client` Outlook Web style client
- `docs/architecture` initial technical decisions
- `docs/licensing` license policy and CI guardrails
- `installation` deployment scripts and documentation

### Getting started

The current skeleton compiles the Rust workspace crates.

```powershell
cargo check
```

On `Debian Trixie`, the installation scripts also deploy a first static administration console through `nginx`, with `/api/` reverse-proxied to the local Rust service.

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
