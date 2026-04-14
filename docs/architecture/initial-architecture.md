# Architecture initiale

## Decisions de depart

- stockage primaire : `PostgreSQL`
- axe protocolaire moderne : `JMAP`
- compatibilite initiale : `SMTP`, `IMAP`
- code LPE : `Apache-2.0`
- dependances : `Apache-2.0` d'abord, `MIT` uniquement par exception justifiee
- architecture de donnees preparee pour une IA locale future

## Vision

`LPE` est un serveur de messagerie et de collaboration moderne. Le coeur metier ne depend pas de `IMAP` ni de `SMTP`. Les protocoles externes sont des adaptateurs autour d'un modele interne stable.

## Blocs principaux

1. `lpe-domain`
Types metier partages.

2. `lpe-core`
Regles applicatives et orchestration du domaine.

3. `lpe-storage`
Adaptateur de persistance `PostgreSQL` et stockage blobs plus tard.

4. `lpe-ai`
Contrats et services pour une IA locale future avec provenance.

5. `lpe-jmap`
Point d'entree moderne pour le client web et les futures apps natives.

6. `lpe-admin-api`
Plan de controle pour le back office.

7. `lpe-cli`
Executable de demarrage local du serveur.

## Priorites MVP

- comptes, domaines, alias, quotas
- SMTP entrant et submission
- IMAP
- webmail HTTPS
- recherche
- administration web
- projections documentaires et artefacts IA locaux
- indexation des pieces jointes `PDF`, `DOCX` et `ODT`
