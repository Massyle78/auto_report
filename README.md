# Auto Report - Pipeline d'Analyse de Données d'Enquêtes

## Description générale

**Auto Report** est un pipeline d'analyse de données conçu pour traiter et analyser des enquêtes d'insertion professionnelle. Le projet permet de générer automatiquement des tableaux croisés dynamiques (TCD) à partir de fichiers Excel contenant des données d'enquêtes, avec des analyses croisées par année, genre, branche et filière.

Le pipeline effectue des analyses statistiques complètes incluant :
- Des comptages par modalités et croisements
- Des agrégations de catégories (régions, tailles d'entreprise)
- Des conversions en pourcentages
- Des calculs de rémunération moyenne
- Des exports multi-formats (Excel, pickle)

## Fonctionnalités principales

- **Analyse globale** : Analyse de l'ensemble des données avec croisements année/genre
- **Analyse par branche** : Analyse segmentée par branche d'ingénierie
- **Analyse par filière** : Analyse segmentée par filière d'ingénierie
- **Post-traitement** : Agrégation de catégories et conversion en pourcentages
- **Calculs de rémunération** : Statistiques de salaire (moyennes AP/HP, France uniquement)
- **Export flexible** : Génération de fichiers Excel (multi-sheets ou single-sheet) et pickle

## Arborescence du répertoire

```
auto_report/
├── data/                                    # Données d'entrée (fichiers Excel)
│   ├── input.xlsx
│   └── ...
├── data-analysis-pipeline/                  # Pipeline principal d'analyse
│   ├── config/
│   │   └── settings.py                      # Configuration centralisée
│   ├── data/                                # Données locales du pipeline
│   │   └── input.xlsx                       # Fichier d'entrée par défaut
│   ├── main.py                              # Point d'entrée principal
│   ├── requirements.txt                     # Dépendances Python
│   ├── reports/                             # Rapports générés
│   │   ├── full_run_*_counts.xlsx
│   │   ├── full_run_*_aggregated.xlsx
│   │   └── full_run_*_percent.xlsx
│   ├── src/
│   │   ├── analysis/                        # Modules d'analyse
│   │   │   ├── global_analysis.py          # Analyse globale
│   │   │   ├── branch_analysis.py          # Analyse par branche
│   │   │   ├── filiere_analysis.py        # Analyse par filière
│   │   │   └── remuneration.py            # Calculs de rémunération
│   │   ├── io/                              # Entrée/Sortie
│   │   │   └── data_writer.py              # Export Excel/pickle
│   │   ├── processing/                      # Traitement des données
│   │   │   ├── data_loader.py              # Chargement et préparation
│   │   │   └── post_processing.py          # Agrégations et pourcentages
│   │   └── utils/                           # Utilitaires
│   │       ├── logging_config.py           # Configuration des logs
│   │       └── sheet_utils.py               # Utilitaires Excel
│   └── tests/                               # Tests unitaires
├── script/                                  # Scripts utilitaires
│   ├── main.py                              # Script alternatif (ancien)
│   ├── aggregate_data.py                    # Agrégation standalone
│   └── aggregate_to_percent.py              # Conversion en pourcentages
├── LICENSE                                  # Licence MIT
├── README.md                                # Ce fichier
├── TODO.txt                                 # Liste des tâches
├── excel_TCD.txt                            # Documentation TCD
└── rapport_descriptif.xlsx                  # Rapport généré (ancien format)
```

## Description des dossiers et fichiers clés

### `data-analysis-pipeline/`

Répertoire principal contenant le pipeline d'analyse moderne et structuré.

#### `config/settings.py`

Fichier de configuration centralisé définissant :
- **Chemins** : `DATA_DIR`, `REPORTS_DIR`, `INPUT_FILE_NAME`
- **Colonnes du domaine** : `YEAR_COL`, `GENDER_COL`, `BRANCH_COL`, `FILIER_COL`
- **Paramètres** : `YEAR_INTERVAL` (intervalle d'années à analyser)
- **Colonnes de résumé** : `SUMMARY_COLUMNS` (liste des colonnes catégorielles à analyser)
- **Colonnes de rémunération** : `SALARY_AP_COL`, `SALARY_HP_COL`

#### `main.py`

Point d'entrée principal du pipeline. Gère :
- Le parsing des arguments en ligne de commande
- L'orchestration des analyses (global, branch, filiere, all)
- L'application des post-traitements (agrégation, pourcentages)
- La sauvegarde des résultats (Excel, pickle)

**Arguments CLI** :
- `--analysis` : Type d'analyse (`global`, `branch`, `filiere`, `all`)
- `--output` : Chemin de base pour les fichiers de sortie
- `--aggregate` : Activer l'agrégation des catégories
- `--percent` : Convertir les résultats en pourcentages
- `--no-pickle` : Désactiver la sauvegarde pickle
- `--single-sheet` : Pour l'analyse globale, écrire tous les pivots sur une seule feuille
- `--input-file` : Nom du fichier Excel d'entrée dans `data/`

#### `src/analysis/`

Modules d'analyse statistique.

**`global_analysis.py`** :
- `run_global_analysis(df, summary_cols)` : Génère des tableaux croisés pour l'ensemble des données
  - **Arguments** : `df` (DataFrame), `summary_cols` (liste de colonnes)
  - **Output** : Dictionnaire `{nom_colonne: DataFrame_pivot}`
  - **Comportement** : Crée des pivots avec index=modalité, colonnes=(année, genre), valeurs=comptages. Ajoute des totaux par année.

**`branch_analysis.py`** :
- `run_branch_analysis(df, summary_cols)` : Analyse segmentée par branche
  - **Arguments** : `df` (DataFrame), `summary_cols` (liste de colonnes)
  - **Output** : Dictionnaire `{nom_colonne: DataFrame_pivot_par_branche}`
  - **Comportement** : Filtre par branche, génère des pivots par branche, combine avec MultiIndex incluant le niveau "Branch".

**`filiere_analysis.py`** :
- `run_filiere_analysis(df, summary_cols)` : Analyse segmentée par filière
  - **Arguments** : `df` (DataFrame), `summary_cols` (liste de colonnes)
  - **Output** : Dictionnaire `{nom_colonne: DataFrame_pivot_par_filiere}`
  - **Comportement** : Similaire à `branch_analysis` mais avec croisement année/filière au lieu de année/genre.

**`remuneration.py`** :
- `build_remuneration_sheets(df)` : Calcule les statistiques de rémunération
  - **Arguments** : `df` (DataFrame)
  - **Output** : Dictionnaire avec clés `"Remuneration"` et `"Remuneration (France)"`
  - **Comportement** : Calcule les moyennes de salaire (AP et HP) par année/genre. Génère une version France uniquement (exclut "Étranger").

#### `src/processing/`

Modules de traitement des données.

**`data_loader.py`** :
- `load_data(file_path)` : Charge un fichier Excel
- `clean_column_names(df)` : Nettoie les noms de colonnes (supprime préfixes numériques)
- `filter_by_year_interval(df, year_col, interval)` : Filtre les données sur un intervalle d'années
- `get_prepared_data(input_dir, input_file_name, extra_cleaners)` : Pipeline complet de chargement et préparation
  - **Arguments** : `input_dir` (Path), `input_file_name` (str), `extra_cleaners` (itérable de fonctions)
  - **Output** : DataFrame préparé
  - **Comportement** : Charge, nettoie, applique des cleaners optionnels, filtre par année.

**`post_processing.py`** :
- `aggregate_employment_regions(sheets_dict)` : Agrège les régions en ['Île-de-France', 'Étranger', 'Province']
  - **Arguments** : `sheets_dict` (dict de DataFrames)
  - **Output** : Dictionnaire modifié en place
  - **Comportement** : Trouve les feuilles contenant "EmploiLieuRegionEtranger", agrège toutes les régions sauf IDF et Étranger en "Province".

- `aggregate_company_size(sheets_dict)` : Agrège les tailles d'entreprise
  - **Arguments** : `sheets_dict` (dict de DataFrames)
  - **Output** : Dictionnaire modifié en place
  - **Comportement** : Combine '0' et 'De 1 à 9' en 'Moins de 10'.

- `convert_all_to_percentages(sheets_dict)` : Convertit les comptages en pourcentages colonne par colonne
  - **Arguments** : `sheets_dict` (dict de DataFrames)
  - **Output** : Nouveau dictionnaire avec pourcentages
  - **Comportement** : Pour chaque DataFrame, divise chaque valeur par la somme de sa colonne, multiplie par 100.

#### `src/io/data_writer.py`

Gestion de l'export des résultats.

- `save_to_pickle(sheets_dict, output_path)` : Sauvegarde en format pickle
- `save_to_excel_multisheet(sheets_dict, output_path)` : Export Excel avec une feuille par DataFrame
- `save_to_excel_singlesheet(sheets_dict, output_path, sheet_name)` : Export Excel avec tous les DataFrames sur une seule feuille
  - **Arguments** : `sheets_dict` (dict), `output_path` (Path), `sheet_name` (str, défaut="Combined")
  - **Comportement** : Concatène verticalement tous les DataFrames avec un index MultiIndex "Sheet", ajuste automatiquement les largeurs de colonnes.

### `script/`

Scripts utilitaires et alternatives.

**`main.py`** : Script alternatif générant `rapport_descriptif.xlsx` avec des tableaux par année et genre.

**`aggregate_data.py`** : Script standalone pour l'agrégation de données depuis un fichier pickle.

**`aggregate_to_percent.py`** : Script standalone pour la conversion en pourcentages depuis un fichier pickle.

## Exemples d'utilisation

### Exemples CLI

#### Analyse globale complète avec agrégation et pourcentages

```bash
python data-analysis-pipeline/main.py --analysis all --output data-analysis-pipeline/reports/full_run --aggregate --percent
```

Cette commande génère :
- `full_run_global_counts.xlsx` et `.pkl` : Comptages bruts pour l'analyse globale
- `full_run_global_aggregated.xlsx` et `.pkl` : Comptages agrégés
- `full_run_global_percent.xlsx` et `.pkl` : Pourcentages
- Même structure pour `branch` et `filiere`

#### Analyse par branche uniquement, sans pickle

```bash
python data-analysis-pipeline/main.py --analysis branch --output data-analysis-pipeline/reports/branch_analysis --no-pickle
```

#### Analyse globale avec fichier d'entrée personnalisé

```bash
python data-analysis-pipeline/main.py --analysis global --output data-analysis-pipeline/reports/custom_run --input-file "CGE-UTC_PGE2025 anomyme.xlsx"
```

#### Analyse globale sur une seule feuille Excel

```bash
python data-analysis-pipeline/main.py --analysis global --output data-analysis-pipeline/reports/single_sheet --single-sheet
```

### Exemples de fonctions Python

#### Utilisation programmatique du pipeline

```python
from pathlib import Path
import pandas as pd
from data_analysis_pipeline.src.processing.data_loader import get_prepared_data
from data_analysis_pipeline.src.analysis.global_analysis import run_global_analysis
from data_analysis_pipeline.config.settings import DATA_DIR, SUMMARY_COLUMNS

# Charger et préparer les données
df = get_prepared_data(input_dir=DATA_DIR, input_file_name="input.xlsx")

# Exécuter une analyse globale
sheets = run_global_analysis(df, SUMMARY_COLUMNS)

# Accéder à un tableau spécifique
situation_table = sheets["Situation"]
print(situation_table)
```

#### Post-traitement manuel

```python
from data_analysis_pipeline.src.processing.post_processing import (
    aggregate_employment_regions,
    aggregate_company_size,
    convert_all_to_percentages
)

# Appliquer l'agrégation des régions
sheets = aggregate_employment_regions(sheets)

# Appliquer l'agrégation des tailles d'entreprise
sheets = aggregate_company_size(sheets)

# Convertir en pourcentages
sheets_pct = convert_all_to_percentages(sheets)
```

#### Export personnalisé

```python
from data_analysis_pipeline.src.io.data_writer import save_to_excel_multisheet
from pathlib import Path

# Sauvegarder avec une feuille par tableau
output_path = Path("reports/custom_output.xlsx")
save_to_excel_multisheet(sheets, output_path)
```

### Exemples de workflow complet

#### Workflow 1 : Analyse complète avec toutes les options

```bash
# 1. Vérifier que le fichier d'entrée est dans data-analysis-pipeline/data/
#    (par défaut: input.xlsx)

# 2. Exécuter l'analyse complète
python data-analysis-pipeline/main.py \
    --analysis all \
    --output data-analysis-pipeline/reports/complete_analysis \
    --aggregate \
    --percent

# 3. Les fichiers sont générés dans data-analysis-pipeline/reports/
#    - complete_analysis_global_counts.xlsx
#    - complete_analysis_global_aggregated.xlsx
#    - complete_analysis_global_percent.xlsx
#    - complete_analysis_branch_counts.xlsx
#    - complete_analysis_branch_aggregated.xlsx
#    - complete_analysis_branch_percent.xlsx
#    - complete_analysis_filiere_counts.xlsx
#    - complete_analysis_filiere_aggregated.xlsx
#    - complete_analysis_filiere_percent.xlsx
```

#### Workflow 2 : Analyse rapide sans post-traitement

```bash
# Analyse globale rapide, comptages bruts uniquement
python data-analysis-pipeline/main.py \
    --analysis global \
    --output data-analysis-pipeline/reports/quick_analysis \
    --no-pickle
```

#### Workflow 3 : Utilisation des scripts utilitaires

```python
# Script d'agrégation standalone
from script.aggregate_data import aggregate_data, save_aggregated_data

# Charger et agréger
aggregated = aggregate_data("path/to/data.pkl")
save_aggregated_data(aggregated, "path/to/output_aggregated.pkl")

# Conversion en pourcentages
from script.aggregate_to_percent import convert_to_percentages, save_percentages_to_excel

percent_sheets = convert_to_percentages(aggregated)
save_percentages_to_excel(percent_sheets, "path/to/output_percent.xlsx")
```

## Prérequis

- **Python** : Version 3.8 ou supérieure
- **Bibliothèques Python** :
  - `pandas >= 2.0.0`
  - `openpyxl >= 3.1.0`
  - `numpy >= 1.24.0`

## Installation

1. **Cloner le dépôt** (ou télécharger les fichiers)

2. **Installer les dépendances** :

```bash
cd data-analysis-pipeline
pip install -r requirements.txt
```

Ou avec un environnement virtuel (recommandé) :

```bash
python -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate
cd data-analysis-pipeline
pip install -r requirements.txt
```

## Exécution / Quickstart

### Démarrage rapide

1. **Placer votre fichier Excel d'entrée** dans `data-analysis-pipeline/data/` et le renommer en `input.xlsx` (ou utiliser `--input-file`)

2. **Exécuter une analyse** :

```bash
cd data-analysis-pipeline
python main.py --analysis global --output reports/my_first_run
```

3. **Consulter les résultats** dans `data-analysis-pipeline/reports/my_first_run_counts.xlsx`

### Configuration

Avant d'exécuter, vous pouvez modifier `config/settings.py` pour :
- Changer le fichier d'entrée par défaut (`INPUT_FILE_NAME`)
- Ajuster l'intervalle d'années analysé (`YEAR_INTERVAL`)
- Modifier la liste des colonnes à analyser (`SUMMARY_COLUMNS`)
- Changer les noms de colonnes du domaine si votre structure de données diffère

### Structure des données d'entrée

Le fichier Excel d'entrée doit contenir au minimum :
- Une colonne d'année (par défaut : `AnneeDiplomeVerifiee`)
- Une colonne de genre (par défaut : `IdentiteSexeVerifie`)
- Les colonnes listées dans `SUMMARY_COLUMNS` (voir `config/settings.py`)

Pour les analyses par branche/filière :
- Colonne de branche (par défaut : `Ecole_Branche_abr`)
- Colonne de filière (par défaut : `Ecole_Filiere_abr`)

## Notes complémentaires / Bonnes pratiques

### Format des fichiers de sortie

- **Fichiers `*_counts.xlsx`** : Comptages bruts (tableaux croisés dynamiques)
- **Fichiers `*_aggregated.xlsx`** : Comptages après agrégation des catégories
- **Fichiers `*_percent.xlsx`** : Pourcentages (colonne par colonne)

### Agrégations automatiques

Lorsque `--aggregate` est activé :
- **Régions** : Toutes les régions sauf "Île-de-France" et "Étranger" sont agrégées en "Province"
- **Tailles d'entreprise** : Les catégories "0" et "De 1 à 9" sont combinées en "Moins de 10"

### Gestion des logs

Les logs sont configurés automatiquement via `src/utils/logging_config.py`. Le niveau par défaut est `INFO`. Les messages incluent :
- Le chargement des données
- L'exécution des analyses
- Les avertissements pour colonnes manquantes
- Les erreurs éventuelles

### Performance

- Pour de gros fichiers (>100k lignes), l'analyse complète (`--analysis all`) peut prendre plusieurs minutes
- Les fichiers pickle sont plus rapides à charger que les fichiers Excel pour les réutilisations
- L'option `--no-pickle` peut être utilisée si vous n'avez pas besoin de réutiliser les données

### Extensibilité

Pour ajouter de nouvelles colonnes à analyser :
1. Modifier `SUMMARY_COLUMNS` dans `config/settings.py`
2. Vérifier que la colonne existe dans vos données d'entrée

Pour créer une nouvelle analyse :
1. Créer un nouveau module dans `src/analysis/`
2. Implémenter une fonction `run_xxx_analysis(df, summary_cols)`
3. Ajouter l'option dans `main.py` (argument `--analysis`)

## Architecture du pipeline

```
┌─────────────────┐
│  Fichier Excel  │
│   (data/)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  data_loader.py │  ◄─── Chargement, nettoyage, filtrage par année
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Analysis      │  ◄─── global_analysis.py
│  Modules       │      branch_analysis.py
│                │      filiere_analysis.py
│                │      remuneration.py
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ post_processing │  ◄─── Agrégation (optionnel)
│                 │      Conversion en % (optionnel)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  data_writer.py │  ◄─── Export Excel / Pickle
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Fichiers de    │
│  sortie         │
│  (reports/)     │
└─────────────────┘
```

## License

MIT License

Copyright (c) 2025 Massyle Oumessaoud

Voir le fichier `LICENSE` pour plus de détails.
