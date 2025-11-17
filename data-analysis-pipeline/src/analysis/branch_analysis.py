import logging
from typing import Dict, List

import pandas as pd

from config.settings import YEAR_COL, GENDER_COL, BRANCH_COL
from src.utils.sheet_utils import safe_sheet_name
from src.analysis.remuneration import build_remuneration_sheets


logger = logging.getLogger(__name__)


def _pivot(df: pd.DataFrame, index_col: str) -> pd.DataFrame:
    return pd.pivot_table(
        df,
        index=index_col,
        columns=[YEAR_COL, GENDER_COL],
        aggfunc="size",
        fill_value=0,
    )


def run_branch_analysis(df: pd.DataFrame, summary_cols: List[str]) -> Dict[str, pd.DataFrame]:
    sheets: Dict[str, pd.DataFrame] = {}
    if BRANCH_COL not in df.columns:
        logger.warning("Branch column %s not found; skipping branch analysis", BRANCH_COL)
        return sheets

    for branch in sorted(pd.Series(df[BRANCH_COL].dropna().unique()).astype(str)):
        df_branch = df[df[BRANCH_COL].astype(str) == branch]
        for col in summary_cols:
            if col not in df_branch.columns:
                logger.warning("Skipping missing column in branch analysis: %s", col)
                continue
            sheet_key = f"{branch}_{col}"
            sheets[safe_sheet_name(sheet_key)] = _pivot(df_branch, col)
        # Add remuneration sheets for this branch
        branch_rem = build_remuneration_sheets(df_branch)
        for name, rem_df in branch_rem.items():
            sheets[safe_sheet_name(f"{branch}_{name}")] = rem_df
    return sheets


