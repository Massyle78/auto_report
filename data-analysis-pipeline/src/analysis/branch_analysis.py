import logging
from collections import defaultdict
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
    per_column_tables: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)
    per_remuneration_tables: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)
    if BRANCH_COL not in df.columns:
        logger.warning("Branch column %s not found; skipping branch analysis", BRANCH_COL)
        return sheets

    for branch in sorted(pd.Series(df[BRANCH_COL].dropna().unique()).astype(str)):
        df_branch = df[df[BRANCH_COL].astype(str) == branch]
        for col in summary_cols:
            if col not in df_branch.columns:
                logger.warning("Skipping missing column in branch analysis: %s", col)
                continue
            per_column_tables[str(col)][branch] = _pivot(df_branch, col)
        # Add remuneration sheets for this branch
        branch_rem = build_remuneration_sheets(df_branch)
        for name, rem_df in branch_rem.items():
            per_remuneration_tables[str(name)][branch] = rem_df

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


