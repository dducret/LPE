# Indexation des pieces jointes v1

## Perimetre

La v1 de `LPE` indexe le texte extrait depuis:

- `PDF`
- `DOCX`
- `ODT`

Les autres formats bureautiques restent hors perimetre v1.

## Bibliotheques retenues

- `pdf_oxide` pour `PDF`
- `docx-lite` pour `DOCX`
- extracteur interne `ODT` base sur ZIP + XML

## Raisons

- respect de la contrainte de licences `Apache-2.0` / `MIT`
- extraction locale cote serveur
- indexation dans `PostgreSQL`
- reutilisable plus tard pour recherche et RAG local

## Strategie

1. detecter le format de la piece jointe
2. extraire le texte brut
3. normaliser le texte
4. stocker le texte dans `attachments.extracted_text`
5. alimenter `attachments.extracted_text_tsv`
6. agreger ensuite la recherche message + pieces jointes

## Hors perimetre v1

- OCR des PDF scannes
- `ODS`, `XLSX`, `PPTX`, `ODP`
- extraction semantique avancee
