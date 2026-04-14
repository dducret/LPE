# LPE Agent Instructions

## Francais

Ce fichier contient les instructions de travail a appliquer a tout agent intervenant sur `LPE`.

### Lecture obligatoire avant tout travail

Avant de commencer une tache, l'agent doit lire au minimum:

1. `README.md`
2. `docs/architecture/initial-architecture.md`
3. `docs/licensing/policy.md`

Selon la tache, l'agent doit aussi lire toute documentation specialisee pertinente, notamment:

- `docs/architecture/local-llm.md`
- `docs/architecture/attachments-v1.md`
- `installation/README.md`

L'agent ne doit pas supposer l'architecture, la politique de licences ou le perimetre produit sans verification dans la documentation.

### Contraintes produit et architecture

- `LPE` est un serveur de messagerie et de collaboration moderne
- le backend est ecrit en Rust
- le stockage primaire est `PostgreSQL`
- `JMAP` est l'axe principal du produit moderne
- `IMAP` et `SMTP` sont des couches de compatibilite
- l'architecture doit rester compatible avec une IA locale future, sans sortie des donnees hors serveur
- la recherche et les modeles de donnees doivent privilegier la performance, en particulier dans `PostgreSQL`

### Contraintes de licence

- tout code source produit dans `LPE` doit etre sous licence `Apache-2.0`
- les dependances `MIT` ne sont acceptees que s'il n'existe pas d'alternative `Apache-2.0` raisonnable
- les dependances `GPL`, `LGPL`, `AGPL`, `SSPL` et licences non standard sont interdites
- tout ajout de dependance doit etre verifie contre `docs/licensing/policy.md`

### Contraintes pieces jointes v1

La v1 supporte l'indexation texte de:

- `PDF`
- `DOCX`
- `ODT`

Ne pas etendre le perimetre v1 a d'autres formats sans mise a jour explicite de la documentation.

### Contraintes multilingues v1

- la documentation projet doit etre maintenue en francais et en anglais
- les interfaces web doivent supporter au minimum `en`, `fr`, `de`, `it`, `es`
- la langue par defaut des interfaces est l'anglais

### Methode de travail

- verifier le contexte documentaire avant toute modification
- ne pas contredire les choix d'architecture deja documentes sans les mettre a jour explicitement
- si une modification change le comportement, le perimetre, les prerequis, l'installation ou l'architecture, mettre a jour la documentation correspondante dans le meme travail
- si une nouvelle regle durable apparait, mettre a jour aussi ce fichier `AGENTS.md`
- si une decision structurelle est prise, preferer une mise a jour de la documentation d'architecture plutot qu'une hypothese implicite dans le code

### Installation et exploitation

- pour Linux, la cible initiale d'installation est `Debian Trixie`
- les scripts d'installation doivent d'abord viser une installation depuis le depot Git
- le support Windows Server sera traite plus tard et ne doit pas etre suppose dans les scripts Linux

### Regle de coherence

Quand le code, la documentation et `AGENTS.md` divergent, l'agent doit:

1. identifier la divergence
2. choisir l'option la plus coherente avec les contraintes utilisateur explicites
3. mettre a jour le code et la documentation ensemble

## English

This file defines the working instructions for any agent operating on `LPE`.

### Mandatory reading before any work

Before starting a task, the agent must read at minimum:

1. `README.md`
2. `docs/architecture/initial-architecture.md`
3. `docs/licensing/policy.md`

Depending on the task, the agent must also read any relevant specialized documentation, especially:

- `docs/architecture/local-llm.md`
- `docs/architecture/attachments-v1.md`
- `installation/README.md`

The agent must not assume the architecture, license policy, or product scope without checking the documentation.

### Product and architecture constraints

- `LPE` is a modern mail and collaboration server
- the backend is written in Rust
- the primary store is `PostgreSQL`
- `JMAP` is the main axis of the modern product
- `IMAP` and `SMTP` are compatibility layers
- the architecture must remain compatible with future local AI without data leaving the server
- search and data models must prioritize performance, especially in `PostgreSQL`

### License constraints

- all source code produced in `LPE` must be licensed under `Apache-2.0`
- `MIT` dependencies are accepted only when no reasonable `Apache-2.0` alternative exists
- `GPL`, `LGPL`, `AGPL`, `SSPL`, and non-standard licenses are forbidden
- every new dependency must be checked against `docs/licensing/policy.md`

### v1 attachment constraints

v1 supports text indexing for:

- `PDF`
- `DOCX`
- `ODT`

Do not extend the v1 scope to other formats without an explicit documentation update.

### v1 multilingual constraints

- project documentation must be maintained in both French and English
- web interfaces must support at least `en`, `fr`, `de`, `it`, and `es`
- the default interface language is English

### Working method

- verify the documentation context before any modification
- do not contradict documented architecture choices without updating them explicitly
- if a change affects behavior, scope, prerequisites, installation, or architecture, update the relevant documentation in the same work
- if a new durable rule appears, update `AGENTS.md` as well
- if a structural decision is made, prefer updating architecture documentation rather than leaving the assumption implicit in code

### Installation and operations

- for Linux, the initial installation target is `Debian Trixie`
- installation scripts must first target deployment from the Git repository
- Windows Server support will be handled later and must not be assumed in Linux scripts

### Consistency rule

When code, documentation, and `AGENTS.md` diverge, the agent must:

1. identify the divergence
2. choose the option most consistent with explicit user constraints
3. update code and documentation together

