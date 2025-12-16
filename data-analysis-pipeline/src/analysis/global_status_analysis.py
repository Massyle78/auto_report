import logging
from collections import defaultdict
from typing import Dict, List

import pandas as pd

from config.settings import YEAR_COL, GENDER_COL, STATUS_COL, STATUS_INITIAL_VAL
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


def run_global_status_analysis(df: pd.DataFrame, summary_cols: List[str]) -> Dict[str, pd.DataFrame]:
    """
    Create pivots for the full dataset, split by Status (Initial vs Autre),
    then by year and gender.
    """
    sheets: Dict[str, pd.DataFrame] = {}
    per_column_tables: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)
    per_remuneration_tables: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)

    if STATUS_COL not in df.columns:
        logger.warning("Status column %s not found; skipping global status analysis", STATUS_COL)
        return sheets

    # Create a mask for initial formation
    # We treat NaN as 'Autre' or filter them? Usually explicit comparison is safer.
    mask_initial = df[STATUS_COL] == STATUS_INITIAL_VAL
    
    # Define groups
    groups = {
        "Initial": df[mask_initial],
        "Autre": df[~mask_initial]
    }

    for status_label, df_status in groups.items():
        # Even if empty, we might want to show it? But pivot will fail or be empty.
        if df_status.empty:
            continue
            
        for col in summary_cols:
            if col not in df_status.columns:
                logger.warning("Skipping missing column in global status analysis: %s", col)
                continue
            
            pivot = _pivot(df_status, col)
            # Add per-year totals
            try:
                years = pivot.columns.get_level_values(0).unique()
                for y in years:
                    pivot[(y, "Total")] = pivot.loc[:, y].sum(axis=1)
                pivot = pivot.sort_index(axis=1, level=[0, 1])
            except Exception:
                logger.debug("Could not add per-year totals for status %s, col %s", status_label, col, exc_info=True)
            
            per_column_tables[str(col)][status_label] = pivot

        # Remuneration
        status_rem = build_remuneration_sheets(df_status, pivot_col=GENDER_COL)
        for name, rem_df in status_rem.items():
            per_remuneration_tables[str(name)][status_label] = rem_df

    def _with_status_level(table: pd.DataFrame, status_name: str) -> pd.DataFrame:
        table_copy = table.copy()
        cols = table_copy.columns
        if isinstance(cols, pd.MultiIndex):
            new_names = ["Status"] + list(cols.names if cols.names is not None else [None] * cols.nlevels)
            tuples = [(status_name,) + tuple(col_tuple) for col_tuple in cols]
        else:
            new_names = ["Status", cols.name]
            tuples = [(status_name, col) for col in cols]
        table_copy.columns = pd.MultiIndex.from_tuples(tuples, names=new_names)
        return table_copy

    def _combine_status_tables(grouped_tables: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, pd.DataFrame]:
        combined_tables: Dict[str, pd.DataFrame] = {}
        for table_name, status_tables in grouped_tables.items():
            frames = []
            # Enforce order: Initial first, then Autre
            for status_name in ["Initial", "Autre"]:
                if status_name in status_tables:
                    frames.append(_with_status_level(status_tables[status_name], status_name))
            
            if not frames:
                continue
                
            combined = pd.concat(frames, axis=1, sort=False).fillna(0)
            combined_tables[safe_sheet_name(table_name)] = combined
        return combined_tables

    sheets.update(_combine_status_tables(per_column_tables))
    sheets.update(_combine_status_tables(per_remuneration_tables))

    return sheets

