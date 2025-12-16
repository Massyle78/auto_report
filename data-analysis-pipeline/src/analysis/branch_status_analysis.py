import logging
from collections import defaultdict
from typing import Dict, List

import pandas as pd

from config.settings import YEAR_COL, GENDER_COL, BRANCH_COL, STATUS_COL, STATUS_INITIAL_VAL
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


def run_branch_status_analysis(df: pd.DataFrame, summary_cols: List[str]) -> Dict[str, pd.DataFrame]:
    """
    Create pivots by Branch, then by Status (Initial vs Autre),
    then by year and gender.
    """
    sheets: Dict[str, pd.DataFrame] = {}
    per_column_tables: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)
    per_remuneration_tables: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)

    if BRANCH_COL not in df.columns:
        logger.warning("Branch column %s not found; skipping branch status analysis", BRANCH_COL)
        return sheets
    
    if STATUS_COL not in df.columns:
        logger.warning("Status column %s not found; skipping branch status analysis", STATUS_COL)
        return sheets

    # Helper to add Status level
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

    for branch in sorted(pd.Series(df[BRANCH_COL].dropna().unique()).astype(str)):
        df_branch = df[df[BRANCH_COL].astype(str) == branch]
        
        # Split by status within this branch
        mask_initial = df_branch[STATUS_COL] == STATUS_INITIAL_VAL
        groups = {
            "Initial": df_branch[mask_initial],
            "Autre": df_branch[~mask_initial]
        }
        
        # Process Summary Columns
        for col in summary_cols:
            status_frames = []
            # Use specific order if desired, or just iterate groups
            for status_label in ["Initial", "Autre"]:
                df_status = groups[status_label]
                if df_status.empty or col not in df_status.columns:
                    continue
                
                pivot = _pivot(df_status, col)
                # Add per-year total
                try:
                    years = pivot.columns.get_level_values(0).unique()
                    for y in years:
                        pivot[(y, "Total")] = pivot.loc[:, y].sum(axis=1)
                    pivot = pivot.sort_index(axis=1, level=[0, 1])
                except Exception:
                    pass
                
                status_frames.append(_with_status_level(pivot, status_label))
            
            if status_frames:
                # Combine statuses for this branch
                branch_col_pivot = pd.concat(status_frames, axis=1, sort=False).fillna(0)
                per_column_tables[str(col)][branch] = branch_col_pivot

        # Process Remuneration
        status_rem_frames: Dict[str, List[pd.DataFrame]] = defaultdict(list)
        for status_label in ["Initial", "Autre"]:
            df_status = groups[status_label]
            if df_status.empty:
                continue
            branch_rem = build_remuneration_sheets(df_status, pivot_col=GENDER_COL)
            for name, rem_df in branch_rem.items():
                status_rem_frames[str(name)].append(_with_status_level(rem_df, status_label))
        
        for name, frames in status_rem_frames.items():
            if frames:
                per_remuneration_tables[name][branch] = pd.concat(frames, axis=1, sort=False).fillna(0)

    # Helper to add Branch level
    def _with_branch_level(table: pd.DataFrame, branch_name: str) -> pd.DataFrame:
        table_copy = table.copy()
        cols = table_copy.columns
        if isinstance(cols, pd.MultiIndex):
            new_names = ["Branch"] + list(cols.names if cols.names is not None else [None] * cols.nlevels)
            tuples = [(branch_name,) + tuple(col_tuple) for col_tuple in cols]
        else:
            new_names = ["Branch", cols.name]
            tuples = [(branch_name, col) for col in cols]
        table_copy.columns = pd.MultiIndex.from_tuples(tuples, names=new_names)
        return table_copy

    def _combine_branch_tables(grouped_tables: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, pd.DataFrame]:
        combined_tables: Dict[str, pd.DataFrame] = {}
        for table_name, branch_tables in grouped_tables.items():
            branch_frames = []
            for branch_name in sorted(branch_tables.keys()):
                branch_frames.append(_with_branch_level(branch_tables[branch_name], branch_name))
            combined = pd.concat(branch_frames, axis=1, sort=False).fillna(0)
            combined_tables[safe_sheet_name(table_name)] = combined
        return combined_tables

    sheets.update(_combine_branch_tables(per_column_tables))
    sheets.update(_combine_branch_tables(per_remuneration_tables))

    return sheets

