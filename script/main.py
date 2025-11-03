import os
import glob
from pathlib import Path
from typing import Dict, List

import pandas as pd


DATA_DIR = Path("data")
DEFAULT_INPUT = DATA_DIR / "CGE-UTC_PGE2024 anonyme.xlsx"
OUTPUT_FILE = "rapport_descriptif.xlsx"

# Mapping from cleaned source column names -> standardized output names
COLUMN_MAPPING: Dict[str, str] = {
    "Situation": "ACTIVITE ACTUELLE",
    "EmploiLieuRegionEtranger": "LIEU DE TRAVAIL",
    "EmploiContrat": "CONTRAT",
    "EmploiFranceCadre": "STATUT CADRE",
    "EmploiEntrepriseTaille": "TAILLE D'ENTREPRISE",
    "Calcul_Euros_EmploiSalaireBrutAnnuelAP": "REMUNERATION (en moyenne) (hors VIE et Thèse)",
    "1erEmploiLapsPourTrouverApresDiplome": "DELAI DE RECHERCHE (1er emploi)",
    "EmploiCommentTrouve": "ACCES AU 1er EMPLOI",
    "EmploiSecteur": "SECTEURS",
    "EmploiService": "SERVICES",
}

STANDARD_COLUMNS: List[str] = [
    "ACTIVITE ACTUELLE",
    "LIEU DE TRAVAIL",
    "CONTRAT",
    "STATUT CADRE",
    "TAILLE D'ENTREPRISE",
    "REMUNERATION (en moyenne) (hors VIE et Thèse)",
    "DELAI DE RECHERCHE (1er emploi)",
    "ACCES AU 1er EMPLOI",
    "SECTEURS",
    "SERVICES",
]

SEX_COLUMN_CLEAN = "IdentiteSexeVerifie"
YEAR_COLUMN_CLEAN = "AnneeDiplomeVerifiee"
SEX_NORMALIZED = "SexeNormalise"
MODALITY_LABEL = "Modality"


def find_input_file() -> Path:
    if DEFAULT_INPUT.exists():
        return DEFAULT_INPUT
    candidates = sorted(glob.glob(str(DATA_DIR / "*.xlsx")))
    if not candidates:
        raise FileNotFoundError("Aucun fichier .xlsx trouvé dans le dossier data/")
    return Path(candidates[0])


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    # Remove leading number and dot pattern: e.g., "15. Situation" -> "Situation"
    cleaned = df.copy()
    cleaned.columns = (
        cleaned.columns
        .str.replace(r"^\s*\d+\.?\s*", "", regex=True)
        .str.strip()
    )
    return cleaned


def apply_column_mapping(df: pd.DataFrame) -> pd.DataFrame:
    # Rename only keys present in df
    intersection = {k: v for k, v in COLUMN_MAPPING.items() if k in df.columns}
    return df.rename(columns=intersection)


def normalize_sex(series: pd.Series) -> pd.Series:
    mapping_homme = {"m", "h", "homme", "male", "masc"}
    mapping_femme = {"f", "femme", "female"}
    def _norm(x):
        if pd.isna(x):
            return None
        s = str(x).strip().lower()
        if s in mapping_homme or s.startswith("homme"):
            return "Homme"
        if s in mapping_femme or s.startswith("femme"):
            return "Femme"
        return None
    return series.apply(_norm)


def build_counts_by_year_and_sex(df: pd.DataFrame, var_col: str, years: List) -> pd.DataFrame:
    # Prepare working copy with modality filled
    work = df[[YEAR_COLUMN_CLEAN, SEX_NORMALIZED, var_col]].copy()
    work[var_col] = work[var_col].fillna("Non renseigné")

    # Determine full set of modalities across all years
    all_modalities = (
        work[var_col]
        .astype("string")
        .value_counts(dropna=False)
        .index
        .tolist()
    )

    # Order modalities by overall frequency descending
    modality_order = (
        work[var_col]
        .astype("string")
        .value_counts(dropna=False)
        .index
        .tolist()
    )

    result = pd.DataFrame({MODALITY_LABEL: modality_order})

    for year in years:
        sub = work[work[YEAR_COLUMN_CLEAN] == year]
        # Counts by modality overall (all sexes included)
        total_counts = (
            sub[var_col]
            .astype("string")
            .value_counts(dropna=False)
            .reindex(modality_order, fill_value=0)
        )
        # Crosstab for Homme/Femme only
        sub_hf = sub[sub[SEX_NORMALIZED].isin(["Homme", "Femme"])]
        ctab = pd.crosstab(
            sub_hf[var_col].astype("string"),
            sub_hf[SEX_NORMALIZED],
        ).reindex(index=modality_order, fill_value=0)
        # Ensure both columns exist
        for sex in ["Homme", "Femme"]:
            if sex not in ctab.columns:
                ctab[sex] = 0
        # Column order and rename with year suffix
        year_suffix = f"_{year}"
        result[f"Homme{year_suffix}"] = ctab["Homme"].values
        result[f"Femme{year_suffix}"] = ctab["Femme"].values
        result[f"Total{year_suffix}"] = total_counts.values

    return result


def main():
    input_file = find_input_file()
    df = pd.read_excel(input_file)

    # Clean columns and apply mapping
    df = clean_column_names(df)
    df = apply_column_mapping(df)

    # Validate presence of year and sex columns
    missing = [c for c in [SEX_COLUMN_CLEAN, YEAR_COLUMN_CLEAN] if c not in df.columns]
    if missing:
        raise KeyError(
            f"Colonnes manquantes dans le fichier: {', '.join(missing)}"
        )

    # Normalize sex
    df[SEX_NORMALIZED] = normalize_sex(df[SEX_COLUMN_CLEAN])

    # Available years
    years = (
        df[YEAR_COLUMN_CLEAN]
        .dropna()
        .unique()
    )
    # Sort years numerically when possible
    try:
        years = sorted(years, key=lambda x: int(x))
    except Exception:
        years = sorted(years)

    # Generate one sheet per standardized variable present
    with pd.ExcelWriter(OUTPUT_FILE) as writer:
        for std_col in STANDARD_COLUMNS:
            if std_col not in df.columns:
                continue
            table = build_counts_by_year_and_sex(df, std_col, years)
            # Place modality column first
            cols = [MODALITY_LABEL] + [
                col for y in years for col in (f"Homme_{y}", f"Femme_{y}", f"Total_{y}")
            ]
            table = table[cols]
            # Excel sheet name limit 31 chars
            sheet_name = std_col[:31]
            table.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Rapport généré: {os.path.abspath(OUTPUT_FILE)}")


if __name__ == "__main__":
    main()
