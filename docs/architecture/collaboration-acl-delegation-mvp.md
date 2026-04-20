# Collaboration ACL and Delegation MVP | MVP ACL et delegation de collaboration

## Francais

### Objectif

Ce document decrit le premier modele MVP de partage, delegation et ACL fines pour les contacts et calendriers `LPE`.

Le MVP reste strictement aligne sur les tables canoniques existantes:

- `contacts`
- `calendar_events`
- `audit_events`

Il n'introduit aucun stockage parallele pour les objets metier. Les droits sont ajoutes comme une couche canonique de collection au-dessus des objets deja possedes par un compte.

### Principes

- les contacts et evenements restent stockes uniquement dans leurs tables canoniques existantes
- le partage et la delegation sont limites a l'interieur d'un meme tenant
- le modele de droits est commun a `JMAP`, `DAV`, au webmail et aux APIs compte
- aucun protocole ne cree son propre modele de partage
- aucun code `Stalwart` n'est reutilise
- les changements de droits reutilisent le journal canonique `audit_events`

### Collection canonique MVP

Chaque compte possede implicitement deux collections canoniques:

- son carnet `contacts` par defaut
- son calendrier `calendar` par defaut

Le MVP ne cree pas encore de collections arbitraires persistantes. Il expose seulement:

- la collection possedee `default` pour le proprietaire
- des collections partagees virtuelles derivees des grants pour les autres comptes du meme tenant

Les objets restent physiquement stockes chez leur proprietaire. Une collection partagee est donc une projection de droits sur les objets canoniques du compte proprietaire, pas une copie.

### Modele de grant

Le MVP introduit une table canonique `collaboration_collection_grants`.

Chaque grant est scope par:

- `tenant_id`
- `collection_kind` dans `contacts` ou `calendar`
- `owner_account_id`
- `grantee_account_id`

Chaque grant porte les droits suivants:

- `may_read`
- `may_write`
- `may_delete`
- `may_share`

Contraintes MVP:

- `owner_account_id` et `grantee_account_id` doivent rester dans le meme tenant
- l'auto-delegation n'est pas autorisee
- `may_write`, `may_delete` et `may_share` impliquent `may_read`
- `may_delete` et `may_share` impliquent aussi `may_write`
- un seul grant existe par couple `(tenant_id, collection_kind, owner_account_id, grantee_account_id)`

### Semantique MVP

Le MVP supporte:

- partage de calendrier entre comptes d'un meme tenant
- partage de contacts entre comptes d'un meme tenant
- delegation minimale lecture/ecriture/suppression/partage sur la collection complete
- exposition coherente des memes droits dans `JMAP` et `DAV`
- audit minimal des changements de grants

Le MVP ne supporte pas encore:

- ACL par objet individuel
- partage inter-tenant
- groupes de partage
- roles hierarchiques complexes de secretaire/mandataire
- abonnement partiel ou filtrage de sous-ensemble
- historique fin de synchronisation des ACL

### Exposition protocolaire

#### JMAP

`JMAP Contacts` et `JMAP Calendars` exposent:

- la collection `default` du compte authentifie
- les collections partagees accessibles via `AddressBook/*` et `Calendar/*`
- les droits `myRights` derives des grants canoniques

`ContactCard/set` et `CalendarEvent/set` peuvent creer dans une collection partagee si `may_write=true`.

#### DAV

`CardDAV` et `CalDAV` exposent:

- `/dav/addressbooks/me/{collection-id}/`
- `/dav/calendars/me/{collection-id}/`

`PROPFIND` depth `1` sur les homes DAV retourne toutes les collections accessibles.

Les lectures et ecritures DAV appliquent les memes grants canoniques que `JMAP`.

### Audit MVP

Le MVP n'ajoute pas de journal specialise. Il reutilise `audit_events`.

Les actions minimales journalisees sont:

- creation ou mise a jour d'un grant de partage
- suppression d'un grant de partage

Le detail des objets modifies reste volontairement minimal dans ce lot.

### Limites volontaires du MVP

- granularite collection uniquement sur les collections implicites par defaut
- pas de collection partagee renommee par utilisateur
- pas de delegation speciale `send-on-behalf`, `send-as` ou workflow de boite partagee mail
- pas de conflits multi-maitres sophistiques
- pas de notification temps reel de changement de droits

## English

### Objective

This document describes the first `LPE` MVP sharing, delegation, and fine ACL model for contacts and calendars.

The MVP stays strictly aligned with the existing canonical tables:

- `contacts`
- `calendar_events`
- `audit_events`

It does not introduce any parallel business-object storage. Rights are added as a canonical collection layer over the objects already owned by an account.

### Principles

- contacts and events remain stored only in their existing canonical tables
- sharing and delegation stay limited to accounts inside the same tenant
- the rights model is shared by `JMAP`, `DAV`, the web client, and account APIs
- no protocol creates its own sharing model
- no `Stalwart` code is reused
- rights changes reuse the canonical `audit_events` journal

### Canonical MVP collection model

Each account implicitly owns two canonical collections:

- its default `contacts` collection
- its default `calendar` collection

The MVP does not yet create arbitrary persistent collections. It only exposes:

- the owned `default` collection for the owner
- virtual shared collections derived from grants for other accounts in the same tenant

Objects remain physically stored with their owner. A shared collection is therefore a rights projection over the owner's canonical objects, not a copy.

### Grant model

The MVP introduces a canonical `collaboration_collection_grants` table.

Each grant is scoped by:

- `tenant_id`
- `collection_kind` in `contacts` or `calendar`
- `owner_account_id`
- `grantee_account_id`

Each grant carries the following rights:

- `may_read`
- `may_write`
- `may_delete`
- `may_share`

MVP constraints:

- `owner_account_id` and `grantee_account_id` must stay inside the same tenant
- self-delegation is not allowed
- `may_write`, `may_delete`, and `may_share` imply `may_read`
- `may_delete` and `may_share` also imply `may_write`
- only one grant exists per `(tenant_id, collection_kind, owner_account_id, grantee_account_id)`

### MVP semantics

The MVP supports:

- calendar sharing between accounts in the same tenant
- contact sharing between accounts in the same tenant
- minimal read/write/delete/share delegation over the full collection
- coherent exposure of the same rights through `JMAP` and `DAV`
- minimal audit of grant changes

The MVP does not yet support:

- per-item ACLs
- cross-tenant sharing
- sharing groups
- complex secretary or proxy delegation roles
- partial subscriptions or subset filtering
- fine-grained ACL sync history

### Protocol exposure

#### JMAP

`JMAP Contacts` and `JMAP Calendars` expose:

- the authenticated account's `default` collection
- accessible shared collections through `AddressBook/*` and `Calendar/*`
- `myRights` values derived from canonical grants

`ContactCard/set` and `CalendarEvent/set` may create into a shared collection when `may_write=true`.

#### DAV

`CardDAV` and `CalDAV` expose:

- `/dav/addressbooks/me/{collection-id}/`
- `/dav/calendars/me/{collection-id}/`

DAV home `PROPFIND` depth `1` returns every accessible collection.

DAV reads and writes apply the same canonical grants as `JMAP`.

### MVP audit

The MVP does not add a specialized journal. It reuses `audit_events`.

The minimally audited actions are:

- share-grant creation or update
- share-grant deletion

The modified-object detail intentionally remains small in this lot.

### Intentional MVP limits

- collection-only granularity on the implicit default collections
- no user-specific renaming of shared collections
- no `send-on-behalf`, `send-as`, or shared-mailbox workflow delegation
- no sophisticated multi-master conflict handling
- no real-time rights-change notification
