from __future__ import annotations

import logging
from typing import Dict

import pandas as pd


from config.settings import REMUNERATION_SHEET_NAME, REMUNERATION_FR_SHEET_NAME
from src.utils.sheet_utils import safe_sheet_name

logger = logging.getLogger(__name__)


def _select_keys_like(sheets: Dict[str, pd.DataFrame], needle: str) -> list[str]:
    return [k for k in sheets.keys() if needle in k]


def aggregate_employment_regions(sheets_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Aggregate regions into ['Île-de-France', 'Étranger', 'Province'] for matching sheets.

    Works when the DataFrame index contains region labels.
    """
    target_keys = _select_keys_like(sheets_dict, "EmploiLieuRegionEtranger")
    for key in target_keys:
        df = sheets_dict[key]
        if df.index.nlevels != 1:
            logger.warning("Skipping aggregation for %s: index is not 1-level", key)
            continue
        idx = df.index.astype(str)
        has_idf = (idx == "Île-de-France").any()
        has_foreign = (idx == "Étranger").any()
        if not (has_idf and has_foreign):
            logger.warning("Expected 'Île-de-France' and 'Étranger' in index for %s", key)
            continue
        idf = df.loc[df.index.astype(str) == "Île-de-France"].sum()
        foreign = df.loc[df.index.astype(str) == "Étranger"].sum()
        others_mask = (idx != "Île-de-France") & (idx != "Étranger")
        province = df.loc[others_mask].sum() if others_mask.any() else df.iloc[0:0].sum()
        new_df = pd.DataFrame([idf, foreign, province], index=["Île-de-France", "Étranger", "Province"]).fillna(0)
        new_df.columns = df.columns
        sheets_dict[key] = new_df
    return sheets_dict


def aggregate_company_size(sheets_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Aggregate size categories: '0' and 'De 1 à 9' -> 'Moins de 10'."""
    target_keys = _select_keys_like(sheets_dict, "EmploiEntrepriseTaille")
    for key in target_keys:
        df = sheets_dict[key]
        if df.index.nlevels != 1:
            logger.warning("Skipping size aggregation for %s: index is not 1-level", key)
            continue
        idx = df.index.astype(str)
        lt10_mask = (idx == "0") | (idx == "De 1 à 9")
        lt10 = df.loc[lt10_mask].sum() if lt10_mask.any() else df.iloc[0:0].sum()
        keep_df = df.loc[~lt10_mask]
        agg_df = pd.concat([pd.DataFrame([lt10], index=["Moins de 10"]).fillna(0), keep_df])
        # Keep a stable order
        sheets_dict[key] = agg_df
    return sheets_dict


def convert_all_to_percentages(sheets_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Convert counts to column-wise percentages for every DataFrame."""
    result: Dict[str, pd.DataFrame] = {}
    
    # Identify remuneration keys to skip (both raw and safe names just in case)
    skip_keys = {
        REMUNERATION_SHEET_NAME,
        safe_sheet_name(REMUNERATION_SHEET_NAME),
        REMUNERATION_FR_SHEET_NAME,
        safe_sheet_name(REMUNERATION_FR_SHEET_NAME),
    }

    for key, df in sheets_dict.items():
        if key in skip_keys:
            # Just copy the average salary table without modification
            result[key] = df.copy()
            continue

        sums = df.sum(axis=0)
        with pd.option_context("mode.use_inf_as_na", True):
            pct = (df.divide(sums, axis=1) * 100).fillna(0.0)
        result[key] = pct
    return result


