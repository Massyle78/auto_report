from pathlib import Path


# Base directories
PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
DATA_DIR: Path = PROJECT_ROOT / "data"
REPORTS_DIR: Path = PROJECT_ROOT / "reports"

# Input file name (place your Excel file in data/)
INPUT_FILE_NAME: str = "input.xlsx"

# Domain column names
YEAR_COL: str = "AnneeDiplomeVerifiee"
GENDER_COL: str = "IdentiteSexeVerifie"
BRANCH_COL: str = "Ecole_Branche_abr"
FILIER_COL: str = "Ecole_Filiere_abr"

# Parameters
YEAR_INTERVAL: int = 2  # inclusive interval: [max_year - YEAR_INTERVAL, max_year]

# List of summary columns to pivot on in analyses
# Adjust this list to include all the categorical columns you want to summarize.
SUMMARY_COLUMNS: list[str] = [
    "Situation",
    "EmploiLieuRegionEtranger",
    "EmploiContrat",
    "EmploiFranceCadre",
    "EmploiEntrepriseTaille",
    "1erEmploiLapsPourTrouverApresDiplome",
    "EmploiCommentTrouve",
    "EmploiSecteur",
    "EmploiService",
]


# Salary and region-related columns (used for remuneration summaries)
SALARY_AP_COL: str = "Calcul_Euros_EmploiSalaireBrutAnnuelAP"
SALARY_HP_COL: str = "Calcul_Euros_EmploiSalaireBrutAnnuelHP"
REGION_FOREIGN_COL: str = "EmploiLieuRegionEtranger"

# Output sheet names for remuneration summaries
REMUNERATION_SHEET_NAME: str = "Remuneration"
REMUNERATION_FR_SHEET_NAME: str = "Remuneration (France)"


