# LPE Agent Instructions

Ce fichier contient les instructions de travail a appliquer a tout agent intervenant sur `LPE`.

## Lecture obligatoire avant tout travail

Avant de commencer une tache, l'agent doit lire au minimum:

1. `README.md`
2. `docs/architecture/initial-architecture.md`
3. `docs/licensing/policy.md`

Selon la tache, l'agent doit aussi lire toute documentation specialisee pertinente, notamment:

- `docs/architecture/local-llm.md`
- `docs/architecture/attachments-v1.md`
- `installation/README.md`

L'agent ne doit pas supposer l'architecture, la politique de licences ou le perimetre produit sans verification dans la documentation.

## Contraintes produit et architecture

- `LPE` est un serveur de messagerie et de collaboration moderne.
- le backend est ecrit en Rust
- le stockage primaire est `PostgreSQL`
- `JMAP` est l'axe principal du produit moderne
- `IMAP` et `SMTP` sont des couches de compatibilite
- l'architecture doit rester compatible avec une IA locale future, sans sortie des donnees hors serveur
- la recherche et les modeles de donnees doivent privilegier la performance, en particulier dans `PostgreSQL`

## Contraintes de licence

- tout code source produit dans `LPE` doit etre sous licence `Apache-2.0`
- les dependances `MIT` ne sont acceptees que s'il n'existe pas d'alternative `Apache-2.0` raisonnable
- les dependances `GPL`, `LGPL`, `AGPL`, `SSPL` et licences non standard sont interdites
- tout ajout de dependance doit etre verifie contre `docs/licensing/policy.md`

## Contraintes pieces jointes v1

La v1 supporte l'indexation texte de:

- `PDF`
- `DOCX`
- `ODT`

Ne pas etendre le perimetre v1 a d'autres formats sans mise a jour explicite de la documentation.

## Methode de travail

- verifier le contexte documentaire avant toute modification
- ne pas contredire les choix d'architecture deja documentes sans les mettre a jour explicitement
- si une modification change le comportement, le perimetre, les prerequis, l'installation ou l'architecture, mettre a jour la documentation correspondante dans le meme travail
- si une nouvelle regle durable apparait, mettre a jour aussi ce fichier `AGENTS.md`
- si une decision structurelle est prise, preferer une mise a jour de la documentation d'architecture plutot qu'une hypothese implicite dans le code

## Installation et exploitation

- pour Linux, la cible initiale d'installation est `Debian Trixie`
- les scripts d'installation doivent d'abord viser une installation depuis le depot Git
- le support Windows Server sera traite plus tard et ne doit pas etre suppose dans les scripts Linux

## Regle de coherence

Quand le code, la documentation et `AGENTS.md` divergent, l'agent doit:

1. identifier la divergence
2. choisir l'option la plus coherente avec les contraintes utilisateur explicites
3. mettre a jour le code et la documentation ensemble

