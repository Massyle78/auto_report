from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from config.settings import (
    YEAR_COL,
    GENDER_COL,
    SALARY_AP_COL,
    SALARY_HP_COL,
    REGION_FOREIGN_COL,
    REMUNERATION_SHEET_NAME,
    REMUNERATION_FR_SHEET_NAME,
)


logger = logging.getLogger(__name__)


def _remuneration_pivot(frame: pd.DataFrame, pivot_col: str = GENDER_COL) -> pd.DataFrame:
    col_ap = SALARY_AP_COL
    col_hp = SALARY_HP_COL

    data = frame[[YEAR_COL, pivot_col]].copy()
    if col_ap in frame.columns:
        data[col_ap] = pd.to_numeric(frame[col_ap], errors="coerce")
    else:
        data[col_ap] = float("nan")

    if col_hp in frame.columns:
        data[col_hp] = pd.to_numeric(frame[col_hp], errors="coerce")
    else:
        data[col_hp] = float("nan")

    grouped = data.groupby([YEAR_COL, pivot_col])[[col_ap, col_hp]].mean()
    per_year = data.groupby([YEAR_COL])[[col_ap, col_hp]].mean()

    years = sorted(grouped.index.get_level_values(0).unique())
    sub_cols = sorted(grouped.index.get_level_values(1).unique())

    columns = []
    values_ap = []
    values_hp = []
    for y in years:
        for sc in sub_cols:
            columns.append((y, sc))
            if (y, sc) in grouped.index:
                row = grouped.loc[(y, sc)]
                values_ap.append(row.get(col_ap))
                values_hp.append(row.get(col_hp))
            else:
                values_ap.append(float("nan"))
                values_hp.append(float("nan"))
        columns.append((y, "Total"))
        if y in per_year.index:
            row_y = per_year.loc[y]
            values_ap.append(row_y.get(col_ap))
            values_hp.append(row_y.get(col_hp))
        else:
            values_ap.append(float("nan"))
            values_hp.append(float("nan"))

    matrix = {
        ("AP"): values_ap,
        ("HP"): values_hp,
    }
    result = pd.DataFrame(matrix, index=pd.MultiIndex.from_tuples(columns, names=[YEAR_COL, pivot_col])).T
    result = result.sort_index(axis=1, level=[0, 1])
    return result


def build_remuneration_sheets(df: pd.DataFrame, pivot_col: str = GENDER_COL) -> Dict[str, pd.DataFrame]:
    sheets: Dict[str, pd.DataFrame] = {}
    if (SALARY_AP_COL not in df.columns) and (SALARY_HP_COL not in df.columns):
        return sheets

    try:
        sheets[REMUNERATION_SHEET_NAME] = _remuneration_pivot(df, pivot_col)
    except Exception:
        logger.warning("Failed to build remuneration pivot", exc_info=True)

    if REGION_FOREIGN_COL in df.columns:
        region_series = df[REGION_FOREIGN_COL].astype(str)
        lower = region_series.str.lower()
        france_mask = (lower != "etranger") & (lower != "étranger")
        df_france = df.loc[france_mask]
        try:
            sheets[REMUNERATION_FR_SHEET_NAME] = _remuneration_pivot(df_france, pivot_col)
        except Exception:
            logger.warning("Failed to build remuneration (France) pivot", exc_info=True)

    return sheets


