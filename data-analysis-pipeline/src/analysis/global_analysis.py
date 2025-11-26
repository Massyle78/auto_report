import logging
from typing import Dict, List

import pandas as pd

from config.settings import YEAR_COL, GENDER_COL
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


def run_global_analysis(df: pd.DataFrame, summary_cols: List[str]) -> Dict[str, pd.DataFrame]:
    """Create pivots for the full dataset by year and gender for each column."""
    sheets: Dict[str, pd.DataFrame] = {}
    for col in summary_cols:
        if col not in df.columns:
            logger.warning("Skipping missing column in global analysis: %s", col)
            continue
        pivot = _pivot(df, col)
        # Add per-year total across genders to match notebook behavior
        try:
            years = pivot.columns.get_level_values(0).unique()
            for y in years:
                pivot[(y, "Total")] = pivot.loc[:, y].sum(axis=1)
            pivot = pivot.sort_index(axis=1, level=[0, 1])
        except Exception:
            logger.debug("Could not add per-year totals for %s", col, exc_info=True)
        sheets[col] = pivot

    # Add remuneration sheets (behaves like another summary table family)
    sheets.update(build_remuneration_sheets(df, pivot_col=GENDER_COL))
    return sheets


