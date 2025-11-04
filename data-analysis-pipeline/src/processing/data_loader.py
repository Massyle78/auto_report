import logging
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

from config.settings import (
    DATA_DIR,
    INPUT_FILE_NAME,
    YEAR_COL,
    YEAR_INTERVAL,
)


logger = logging.getLogger(__name__)


def load_data(file_path: Path) -> pd.DataFrame:
    logger.info("Loading Excel: %s", file_path)
    return pd.read_excel(file_path)


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Strip numeric prefixes and surrounding whitespace from column names."""
    def _clean(name: str) -> str:
        if not isinstance(name, str):
            return name
        # Remove leading digits and spaces like '1) ', '12 - '
        name = re.sub(r"^\s*\d+\s*[\).:-]?\s*", "", name)
        return name.strip()

    df = df.copy()
    df.columns = [_clean(col) for col in df.columns]
    return df


def filter_by_year_interval(df: pd.DataFrame, year_col: str, interval: int) -> pd.DataFrame:
    if year_col not in df.columns:
        raise KeyError(f"Missing year column: {year_col}")
    years = pd.to_numeric(df[year_col], errors="coerce").dropna().astype(int)
    if years.empty:
        raise ValueError("No valid years found in the dataset")
    max_year = years.max()
    min_year = max_year - interval
    logger.info("Filtering years between %s and %s (inclusive)", min_year, max_year)
    mask = df[year_col].between(min_year, max_year)
    return df.loc[mask].copy()


def get_prepared_data(
    input_dir: Path | None = None,
    input_file_name: str | None = None,
    extra_cleaners: Iterable | None = None,
) -> pd.DataFrame:
    """Load, clean column names, and filter by year interval.

    extra_cleaners: optional iterables of callables(df)->df applied after basic clean.
    """
    directory = input_dir or DATA_DIR
    file_name = input_file_name or INPUT_FILE_NAME
    file_path = directory / file_name
    df = load_data(file_path)
    df = clean_column_names(df)
    if extra_cleaners:
        for func in extra_cleaners:
            df = func(df)
    df = filter_by_year_interval(df, YEAR_COL, YEAR_INTERVAL)
    return df


