# LPE Development Agent Context

## Language Rule

This document is intentionally maintained in English and French.

Agents must not read both language versions and do not need to compare them. The English version is authoritative and takes precedence if there is any divergence. The French version is provided only to improve understanding for humans whose native language is French.

## English

This file provides architecture context for any AI agent implementing `LPE`.

### Documents to read before any change

Before changing code, the agent must read:

1. `README.md`
2. `ARCHITECTURE.md`
3. `docs/architecture/initial-architecture.md`
4. `docs/licensing/policy.md`

Depending on the task, also read the relevant specialized documentation, especially:

- `docs/architecture/local-llm.md`
- `docs/architecture/attachments-v1.md`
- `installation/README.md`

The agent must not assume the architecture, license policy, or product scope without checking the documentation.

### Stalwart reference

Stalwart is an important functional reference for `LPE`.

Its approach is close to the product goal:

- modern mail and collaboration server
- Rust implementation
- support for `JMAP`, `IMAP`, `SMTP`, `CalDAV`, `CardDAV`, `WebDAV`
- multi-tenancy
- web administration
- search
- security
- observability
- flexible storage
- integrated anti-spam and anti-abuse capabilities

However, the agent must not copy Stalwart or treat its architecture as directly applicable to `LPE`.

Mandatory divergences are:

- `LPE` source code must remain under `Apache-2.0`
- all source code produced in `LPE` must be licensed under `Apache-2.0`
- `MIT` dependencies are accepted only when no reasonable `Apache-2.0` alternative exists
- `AGPL`, `LGPL`, `GPL`, `SSPL`, and non-standard licenses are forbidden
- every new dependency must be checked against `docs/licensing/policy.md`
- Stalwart uses an `AGPL-3.0` and proprietary enterprise licensing model, so its code must not be reused in `LPE`
- Stalwart may only be studied as a product or architecture benchmark
- every external dependency or implementation idea must be checked against `docs/licensing/policy.md`

### Non-negotiable LPE architecture

`LPE` has two separated responsibility areas:

- the core `LPE` server, responsible for mailboxes, contacts, calendars, to-do lists, storage, search, rights, and user-visible views
- the `LPE-CT` sorting center in the `DMZ`, responsible for inbound and outbound `SMTP` transport, filtering, quarantine, traceability, relay, and perimeter security

The sorting center is shared across domains and has its own administrators.

The core `LPE` server is multi-tenant. Each tenant manages its domain and domain mailboxes. `LPE` has global administrators and tenant administrators.

### Protocols

The main modern protocol for `LPE` is `JMAP`.

`IMAP` is a mailbox compatibility layer.

Internet-facing `SMTP` must not be moved back into the core `LPE` server when the sorting center already handles inbound and outbound SMTP.

The sorting center is responsible for:

- SMTP ingress from the Internet
- outbound relay
- authenticated outbound handoff reception from `LPE`
- authenticated final delivery toward `LPE`
- `DKIM` signing
- `SPF` and `DMARC` related policies
- retries
- outbound queue
- bounce and `DSN`

The core `LPE` server remains responsible for the canonical sent-message copy in the `Sent` view.

### Outlook and adoption

Native Outlook and mobile support is critical for `LPE` adoption.

The agent must therefore treat Outlook compatibility as a first-class requirement, not as an optional later enhancement.

`LPE` targets `ActiveSync` as the first native Outlook and mobile compatibility layer.

`EWS` remains a future extension and must be evaluated only after the canonical `LPE` submission and synchronization model is stabilized.

Plain `IMAP` + `SMTP` + autodiscover must not be assumed sufficient for Outlook adoption.

Any message sent from Outlook, iPhone Mail, or another native client must be correctly recorded in `LPE` and visible in the `Sent` view.

Every client layer must use the canonical `LPE` submission and synchronization model. No client layer may write its own parallel `Sent` or `Outbox` logic.

Every file entering through an external connection or through a client must be validated with Google `Magika` before normal processing. This applies to both `LPE-CT` and `LPE`, including mail attachments, browser uploads, `JMAP` blobs, `PST` imports, and future file-ingress paths.

### Storage and data constraints

The primary store is `PostgreSQL`.

Search uses `PostgreSQL` by default. A separate search engine may be defined later for large infrastructures.

Identical attachments are deduplicated per domain, but export must reconstruct messages with their blobs.

Large mailboxes follow the thresholds documented in `ARCHITECTURE.md`:

- below `10 GB`: shared database
- above `10 GB`: dedicated database
- above `50 GB`: technical partitioning invisible to the user
- above `100 GB`: online archive with degraded performance

### Security and privacy

`Bcc` is protected metadata.

It must not be indexed in user search or exposed to user-facing AI pipelines.

It may be retained in the internal delivery store for audit and compliance, with explicit rights.

Encryption at rest is optional and may apply to message bodies, attachments, indexes, archives, and sensitive metadata.

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

### Local AI

The architecture must remain compatible with future local AI.

No AI feature may assume that data leaves the server.

AI pipelines must not expose protected data such as `Bcc` or audit-only metadata.

### Working method

- verify the documentation context before any modification
- do not contradict documented architecture choices without updating them explicitly
- if a change affects behavior, scope, prerequisites, installation, or architecture, update the relevant documentation in the same work
- if a new durable rule appears, also update `AGENTS.md`
- if a structural decision is made, prefer updating architecture documentation rather than leaving the assumption implicit in code
- for administration UI lists in `LPE` and `LPE-CT`, use the default management pattern: full-width list, primary `New` or `Create` action in the list header, and a right-side drawer modal for item creation, details, and contextual actions
- avoid persistent side-by-side create forms for primary list management unless the object is not list-oriented or a specialized operational view is clearly more efficient

### Installation and operations

- for Linux, the initial installation target is `Debian Trixie`
- installation scripts must first target deployment from the Git repository
- Windows Server support will be handled later and must not be assumed in Linux scripts

### Consistency rule

When code, documentation, and `AGENTS.md` diverge, the agent must:

1. identify the divergence
2. choose the option most consistent with explicit user constraints
3. update code and documentation together

## Francais

Ce fichier donne le contexte d'architecture a respecter par tout agent IA charge du developpement de `LPE`.

### Regle de langue

Ce document est volontairement maintenu en anglais et en francais.

L'agent ne doit pas lire les deux versions et n'a pas besoin de les comparer. La version anglaise prime en cas de divergence. La version francaise est fournie uniquement pour faciliter la comprehension des hommes et femmes dont la langue maternelle est le francais.

### Documents a lire avant toute modification

Avant de modifier le code, l'agent doit lire:

1. `README.md`
2. `ARCHITECTURE.md`
3. `docs/architecture/initial-architecture.md`
4. `docs/licensing/policy.md`

Selon la tache, lire aussi les documents specialises pertinents, notamment:

- `docs/architecture/local-llm.md`
- `docs/architecture/attachments-v1.md`
- `installation/README.md`

L'agent ne doit pas supposer l'architecture, la politique de licences ou le perimetre produit sans verification dans la documentation.

### Reference Stalwart

Stalwart est une reference fonctionnelle importante pour `LPE`.

Son approche est proche de l'objectif produit:

- serveur mail et collaboration moderne
- implementation en Rust
- support de `JMAP`, `IMAP`, `SMTP`, `CalDAV`, `CardDAV`, `WebDAV`
- multi-tenancy
- administration web
- recherche
- securite
- observabilite
- stockage flexible
- anti-spam et anti-abuse integres

Cependant, l'agent ne doit pas copier Stalwart ni traiter son architecture comme directement applicable a `LPE`.

Les divergences obligatoires sont:

- `LPE` doit rester sous licence source `Apache-2.0`
- tout code source produit dans `LPE` doit etre sous licence `Apache-2.0`
- les dependances `MIT` ne sont acceptees que s'il n'existe pas d'alternative `Apache-2.0` raisonnable
- les licences `AGPL`, `LGPL`, `GPL`, `SSPL` et licences non standard sont interdites
- tout ajout de dependance doit etre verifie contre `docs/licensing/policy.md`
- Stalwart est sous modele `AGPL-3.0` et licence entreprise proprietaire, donc son code ne doit pas etre reutilise dans `LPE`
- Stalwart peut etre etudie comme benchmark produit ou architectural uniquement
- toute dependance ou extrait d'implementation inspire d'un projet externe doit etre verifie contre `docs/licensing/policy.md`

### Architecture LPE non negociable

`LPE` est compose de deux responsabilites separees:

- le coeur `LPE`, charge des boites aux lettres, contacts, calendriers, todo lists, stockage, recherche, droits et vues utilisateur
- le centre de tri `LPE-CT` en `DMZ`, charge du transport `SMTP` entrant et sortant, filtrage, quarantaine, trace, relais et securite perimetrique

Le centre de tri est mutualise entre domaines et possede ses propres administrateurs.

Le coeur `LPE` est multi-tenant. Chaque tenant gere son domaine et les boites du domaine. `LPE` a des administrateurs globaux et des administrateurs par tenant.

### Protocoles

Le protocole moderne principal de `LPE` est `JMAP`.

`IMAP` est une couche de compatibilite mailbox.

`SMTP` Internet-facing ne doit pas etre replace dans le coeur `LPE` si le centre de tri assure deja l'entree et la sortie SMTP.

Le centre de tri est responsable de:

- entree SMTP depuis Internet
- relais sortant
- reception authentifiee du handoff sortant depuis `LPE`
- remise finale authentifiee vers `LPE`
- signature `DKIM`
- politiques liees a `SPF` et `DMARC`
- retries
- queue sortante
- bounce et `DSN`

Le coeur `LPE` reste responsable de la copie canonique du message envoye dans la vue `Sent`.

### Outlook et adoption

Le support Outlook/mobile natif est un point critique pour l'adoption de `LPE`.

L'agent doit donc traiter la compatibilite Outlook comme une exigence de premier plan, pas comme une amelioration optionnelle.

`LPE` cible `ActiveSync` comme premiere couche de compatibilite native Outlook/mobile.

`EWS` reste une extension future et doit etre evalue uniquement apres stabilisation du modele canonique `LPE` de soumission et de synchronisation.

Une simple combinaison `IMAP` + `SMTP` + autodiscover ne doit pas etre supposee suffisante pour l'adoption Outlook.

Tout envoi effectue depuis un client Outlook, iPhone Mail ou autre client natif doit etre enregistre correctement dans `LPE` et visible dans la vue `Sent`.

Toutes les couches clientes doivent utiliser le modele canonique `LPE` de soumission et de synchronisation. Aucune couche cliente ne doit ecrire une logique `Sent` ou `Outbox` parallele.

Tout fichier entrant via connexion externe ou via un client doit etre valide par Google `Magika` avant traitement normal. Cela s'applique a la fois a `LPE-CT` et a `LPE`, y compris aux pieces jointes mail, uploads navigateur, blobs `JMAP`, imports `PST` et futurs points d'entree de fichiers.

### Contraintes de stockage et donnees

Le stockage principal est `PostgreSQL`.

La recherche utilise `PostgreSQL` par defaut. Un moteur separe peut etre defini plus tard pour grandes infrastructures.

Les pieces jointes identiques sont dedupliquees par domaine, mais l'export doit reconstruire les messages avec leurs blobs.

Les grosses boites suivent les seuils documentes dans `ARCHITECTURE.md`:

- moins de `10 GB`: base partagee
- plus de `10 GB`: base dediee
- plus de `50 GB`: partition technique invisible a l'utilisateur
- plus de `100 GB`: archive en ligne a performances degradees

### Securite et confidentialite

Le champ `Bcc` est une metadonnee protegee.

Il ne doit pas etre indexe dans la recherche utilisateur ni expose aux pipelines IA utilisateur.

Il peut etre conserve dans le store interne de livraison pour audit/compliance, avec droits explicites.

Le chiffrement au repos est facultatif et peut s'appliquer aux corps de messages, pieces jointes, index, archives et metadonnees sensibles.

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

### IA locale

L'architecture doit rester compatible avec une IA locale future.

Aucune fonctionnalite IA ne doit supposer une sortie des donnees hors serveur.

Les pipelines IA ne doivent pas exposer de donnees protegees comme `Bcc` ou des metadonnees reservees a l'audit.

### Methode de travail

- verifier le contexte documentaire avant toute modification
- ne pas contredire les choix d'architecture deja documentes sans les mettre a jour explicitement
- si une modification change le comportement, le perimetre, les prerequis, l'installation ou l'architecture, mettre a jour la documentation correspondante dans le meme travail
- si une nouvelle regle durable apparait, mettre a jour aussi `AGENTS.md`
- si une decision structurelle est prise, preferer une mise a jour de la documentation d'architecture plutot qu'une hypothese implicite dans le code
- pour les listes d'administration dans `LPE` et `LPE-CT`, utiliser le pattern de gestion par defaut: liste pleine largeur, action principale `New` ou `Create` dans l'entete de liste, et drawer modal lateral droit pour la creation, les details et les actions contextuelles
- eviter les formulaires de creation persistants cote a cote pour la gestion principale des listes, sauf si l'objet n'est pas oriente liste ou si une vue operationnelle specialisee est clairement plus efficace

### Installation et exploitation

- pour Linux, la cible initiale d'installation est `Debian Trixie`
- les scripts d'installation doivent d'abord viser une installation depuis le depot Git
- le support Windows Server sera traite plus tard et ne doit pas etre suppose dans les scripts Linux

### Regle de coherence

Quand le code, la documentation et `AGENTS.md` divergent, l'agent doit:

1. identifier la divergence
2. choisir l'option la plus coherente avec les contraintes utilisateur explicites
3. mettre a jour le code et la documentation ensemble
