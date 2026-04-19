# Tasks MVP | MVP Taches

## Francais

### Objectif

Ce document decrit le premier modele canonique des taches/to-do implemente dans `LPE`.

Le scope MVP ajoute une base metier unique pour les taches personnelles de compte, sans introduire de logique parallele par protocole. Les futurs adaptateurs `JMAP Tasks`, `DAV` et mobiles devront reutiliser ce meme stockage et ces memes regles d'acces.

### Principes d'architecture

- `PostgreSQL` reste le store primaire
- les taches sont stockees dans une table canonique `tasks`
- chaque tache est possedee par un seul `account_id`
- les droits MVP sont portes par le compte authentifie; aucun modele de droits parallele n'est introduit
- l'API interne manipule directement le modele canonique
- aucun code `Stalwart` n'est reutilise

### Modele de donnees MVP

La table `tasks` expose les champs suivants:

- `id`: identifiant `UUID` canonique
- `tenant_id`: scope multi-tenant interne
- `account_id`: compte proprietaire et frontiere de droits MVP
- `title`: titre utilisateur obligatoire
- `description`: description texte libre
- `status`: `needs-action`, `in-progress`, `completed`, `cancelled`
- `due_at`: echeance optionnelle en `TIMESTAMPTZ`
- `completed_at`: horodatage de completion optionnel en `TIMESTAMPTZ`
- `sort_order`: ordre de presentation stable pour les futurs clients
- `created_at`, `updated_at`: metadonnees de suivi et base future pour la sync

Le choix des statuts reprend une base compatible avec `VTODO` et exploitable plus tard par `JMAP Tasks` et des clients mobiles.

### API interne MVP

Les endpoints comptes suivants sont exposes par `lpe-admin-api`:

- `GET /api/mail/tasks`
- `GET /api/mail/tasks/{task_id}`
- `POST /api/mail/tasks`
- `DELETE /api/mail/tasks/{task_id}`

Le endpoint `POST /api/mail/tasks` sert d'upsert MVP:

- creation si `id` est absent
- mise a jour si `id` est fourni et appartient au compte authentifie

Le workspace `/api/mail/workspace` inclut maintenant aussi `tasks` pour fournir une vue client unifiee.

### Regles MVP

- le titre est obligatoire
- le statut vide est normalise vers `needs-action`
- `completed_at` n'est conserve que pour le statut `completed`
- si une tache passe a `completed` sans date explicite, `LPE` renseigne `completed_at`
- lecture, ecriture et suppression sont toujours bornees au compte authentifie

### Preparation des futurs adaptateurs

Le MVP prepare explicitement:

- une projection canonique reusable par un futur `JMAP Tasks`
- une base compatible avec une future couche `DAV` orientee `VTODO`
- une future exposition mobile et `ActiveSync` sans remodeler le stockage
- une future sync incrementale via `updated_at` et `sort_order`

### Hors scope MVP

- listes de taches partagees
- delegation multi-compte
- recurrence
- sous-taches
- pieces jointes de taches
- rappels et alarmes
- protocoles `JMAP Tasks`, `VTODO`, ou `ActiveSync Tasks` exposes

## English

### Objective

This document describes the first canonical tasks and to-do model implemented in `LPE`.

The MVP adds one internal business model for personal account-scoped tasks without introducing any protocol-specific parallel logic. Future `JMAP Tasks`, `DAV`, and mobile adapters must reuse the same store and access rules.

### Architectural principles

- `PostgreSQL` remains the primary store
- tasks are stored in one canonical `tasks` table
- each task is owned by one `account_id`
- MVP rights are enforced through the authenticated mailbox account; no parallel rights model is introduced
- the internal API writes directly to the canonical model
- no `Stalwart` code is reused

### MVP data model

The `tasks` table exposes the following fields:

- `id`: canonical `UUID`
- `tenant_id`: internal multi-tenant scope
- `account_id`: owning account and MVP rights boundary
- `title`: required user-facing title
- `description`: free-text description
- `status`: `needs-action`, `in-progress`, `completed`, `cancelled`
- `due_at`: optional `TIMESTAMPTZ` due date
- `completed_at`: optional `TIMESTAMPTZ` completion timestamp
- `sort_order`: stable presentation ordering for future clients
- `created_at`, `updated_at`: tracking metadata and future sync anchors

The status set is intentionally aligned with a future `VTODO` mapping and remains reusable for later `JMAP Tasks` and mobile clients.

### MVP internal API

The following account-scoped endpoints are exposed by `lpe-admin-api`:

- `GET /api/mail/tasks`
- `GET /api/mail/tasks/{task_id}`
- `POST /api/mail/tasks`
- `DELETE /api/mail/tasks/{task_id}`

`POST /api/mail/tasks` is the MVP upsert endpoint:

- create when `id` is absent
- update when `id` is present and belongs to the authenticated account

The `/api/mail/workspace` payload now also includes `tasks` so clients can load one unified mailbox and collaboration workspace snapshot.

### MVP rules

- title is required
- an empty status is normalized to `needs-action`
- `completed_at` is retained only for `completed`
- when a task moves to `completed` without an explicit completion timestamp, `LPE` fills `completed_at`
- read, write, and delete operations are always scoped to the authenticated account

### Future adapter preparation

The MVP explicitly prepares:

- a canonical projection reusable by future `JMAP Tasks`
- a storage base compatible with a future `DAV` `VTODO` layer
- future mobile and `ActiveSync` reuse without reshaping the storage model
- future incremental synchronization through `updated_at` and `sort_order`

### Out of scope for the MVP

- shared task lists
- cross-account delegation
- recurrence
- subtasks
- task attachments
- reminders and alarms
- exposed `JMAP Tasks`, `VTODO`, or `ActiveSync Tasks` protocol adapters
