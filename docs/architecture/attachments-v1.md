# Indexation des pieces jointes v1 | v1 attachment indexing

## Francais

### Perimetre

La v1 de `LPE` indexe le texte extrait depuis:

- `PDF`
- `DOCX`
- `ODT`

Les autres formats bureautiques restent hors perimetre v1.

### Bibliotheques retenues

- `pdf_oxide` pour `PDF`
- `docx-lite` pour `DOCX`
- extracteur interne `ODT` base sur ZIP et XML

### Raisons

- respect de la contrainte de licences `Apache-2.0` et `MIT`
- extraction locale cote serveur
- indexation dans `PostgreSQL`
- reutilisable plus tard pour recherche et RAG local

### Strategie

1. valider le type de fichier entrant avec Google `Magika` a partir des bytes reels
2. comparer le type detecte avec le MIME declare et l'extension
3. appliquer la politique d'acceptation, de restriction, de quarantaine ou de rejet
4. detecter le format de la piece jointe retenue
5. extraire le texte brut
6. normaliser le texte
7. stocker le texte dans `attachments.extracted_text`
8. alimenter `attachments.extracted_text_tsv`
9. agreger ensuite la recherche message et pieces jointes

### Implementation v1 actuelle

La premiere implementation fonctionnelle couvre:

- la livraison entrante depuis `LPE-CT` quand `LPE` recoit un message RFC822 complet
- `JMAP Email/import` a partir d'un blob RFC822 televerse
- l'extraction texte reelle pour `PDF`, `DOCX` et `ODT`
- le remplissage de `attachments.extracted_text` et `attachments.extracted_text_tsv`
- l'agregation de la recherche message avec le texte extrait des pieces jointes
- une premiere deduplication des blobs par domaine via un magasin `attachment_blobs`
- la reconstruction des pieces jointes a l'export `PST` bootstrap via les blobs dedupliques

La deduplication est strictement limitee au domaine derive de l'adresse primaire du compte. Deux domaines distincts ne partagent pas leurs blobs, meme si le contenu binaire est identique.

Chaque message conserve ses lignes `attachments` propres pour la semantique mailbox, la recherche et l'export, tandis que les bytes binaires peuvent etre mutualises dans `attachment_blobs`.

### Limites v1 actuelles

- les pieces jointes ne sont indexees que sur les flux ou `LPE` recoit deja le MIME complet
- les brouillons canoniques et la soumission sortante n'exposent pas encore un modele complet d'attachements utilisateur
- les formats hors `PDF`, `DOCX`, `ODT` peuvent etre conserves comme blobs, mais ne sont pas indexes
- aucun `OCR` n'est tente sur les `PDF` images/scannes

La validation `Magika` est obligatoire pour toute piece jointe ou fichier entrant via connexion externe ou via un client avant indexation, import, ou parsing specialise.

### Hors perimetre v1

- OCR des PDF scannes
- `ODS`, `XLSX`, `PPTX`, `ODP`
- extraction semantique avancee

## English

### Scope

`LPE` v1 indexes extracted text from:

- `PDF`
- `DOCX`
- `ODT`

Other office formats remain out of scope for v1.

### Selected libraries

- `pdf_oxide` for `PDF`
- `docx-lite` for `DOCX`
- internal `ODT` extractor based on ZIP and XML

### Reasons

- respects the `Apache-2.0` and `MIT` license constraint
- server-side local extraction
- indexing in `PostgreSQL`
- reusable later for search and local RAG

### Strategy

1. validate the incoming file type with Google `Magika` from the actual bytes
2. compare the detected type with the declared MIME type and extension
3. apply the acceptance, restriction, quarantine, or rejection policy
4. detect the attachment format for the retained file
5. extract raw text
6. normalize the text
7. store text in `attachments.extracted_text`
8. populate `attachments.extracted_text_tsv`
9. aggregate message and attachment search afterward

### Current v1 implementation

The first functional implementation covers:

- inbound delivery from `LPE-CT` when `LPE` receives a full RFC822 message
- `JMAP Email/import` from an uploaded RFC822 blob
- real text extraction for `PDF`, `DOCX`, and `ODT`
- population of `attachments.extracted_text` and `attachments.extracted_text_tsv`
- aggregation of message search with extracted attachment text
- an initial domain-scoped blob deduplication layer through `attachment_blobs`
- bootstrap `PST` export reconstruction of attachments through deduplicated blobs

Deduplication is strictly limited to the domain derived from the account primary email address. Two distinct domains never share attachment blobs even when the binary content is identical.

Each message still keeps its own `attachments` rows for mailbox semantics, search, and export, while the binary payload bytes may be shared in `attachment_blobs`.

### Current v1 limitations

- attachments are indexed only on flows where `LPE` already receives the complete MIME message
- canonical drafts and outbound submission do not yet expose a full end-user attachment model
- formats outside `PDF`, `DOCX`, and `ODT` may be retained as blobs but are not indexed
- no `OCR` is attempted for scanned or image-only `PDF` content

`Magika` validation is mandatory for every attachment or file entering through an external connection or through a client before indexing, import, or specialized parsing.

### Out of scope for v1

- OCR for scanned PDFs
- `ODS`, `XLSX`, `PPTX`, `ODP`
- advanced semantic extraction
