from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from config.settings import (
    DATA_DIR,
    REPORTS_DIR,
    INPUT_FILE_NAME,
    SUMMARY_COLUMNS,
)
from src.utils.logging_config import setup_logging
from src.processing.data_loader import get_prepared_data
from src.analysis.global_analysis import run_global_analysis
from src.analysis.branch_analysis import run_branch_analysis
from src.analysis.filiere_analysis import run_filiere_analysis
from src.processing.post_processing import (
    aggregate_company_size,
    aggregate_employment_regions,
    convert_all_to_percentages,
)
from src.io.data_writer import (
    save_to_pickle,
    save_to_excel_multisheet,
    save_to_excel_single_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Data Analysis Pipeline")
    parser.add_argument(
        "--analysis",
        required=True,
        choices=["global", "branch", "filiere", "all"],
        help="Which analysis to run",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Base output path (without extension). e.g., reports/run_2025",
    )
    parser.add_argument(
        "--aggregate",
        action="store_true",
        help="Apply aggregation post-processing steps",
    )
    parser.add_argument(
        "--percent",
        action="store_true",
        help="Convert outputs to column-wise percentages",
    )
    parser.add_argument(
        "--no-pickle",
        action="store_true",
        help="Skip saving pickle outputs",
    )
    parser.add_argument(
        "--single-sheet",
        action="store_true",
        help="For global analysis: write all pivots on a single sheet",
    )
    parser.add_argument(
        "--input-file",
        default=INPUT_FILE_NAME,
        help="Input Excel file name in data/ directory",
    )
    return parser.parse_args()


def maybe_post_process(sheets: dict[str, pd.DataFrame], do_agg: bool, to_percent: bool) -> dict[str, pd.DataFrame]:
    if do_agg:
        sheets = aggregate_employment_regions(sheets)
        sheets = aggregate_company_size(sheets)
    if to_percent:
        sheets = convert_all_to_percentages(sheets)
    return sheets


def run_analysis(kind: str, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if kind == "global":
        return run_global_analysis(df, SUMMARY_COLUMNS)
    if kind == "branch":
        return run_branch_analysis(df, SUMMARY_COLUMNS)
    if kind == "filiere":
        return run_filiere_analysis(df, SUMMARY_COLUMNS)
    raise ValueError(f"Unknown analysis kind: {kind}")


def main() -> None:
    setup_logging()
    args = parse_args()
    logging.info("Loading and preparing data...")
    df = get_prepared_data(input_dir=DATA_DIR, input_file_name=args.input_file)

    outputs: dict[str, dict[str, pd.DataFrame]] = {}
    kinds = [args.analysis] if args.analysis != "all" else ["global", "branch", "filiere"]
    for kind in kinds:
        
        logging.info("Running %s analysis", kind)
        # Step 1: counts (raw pivots)
        sheets_counts = run_analysis(kind, df)
        outputs[kind] = sheets_counts

    base_out = Path(args.output)
    def save_all_steps(kind: str, base: Path, sheets_counts: dict[str, pd.DataFrame]) -> None:
        # Base name per kind
        base_for_kind = base if args.analysis != "all" else base.with_name(f"{base.name}_{kind}")

        # 1) Counts (TCD only numbers)
        out_counts_xlsx = base_for_kind.with_name(f"{base_for_kind.name}_counts").with_suffix(".xlsx")
        if kind == "global" and args.single_sheet:
            # single-sheet applies only to main consolidated; for step files we keep per-sheet like notebook
            save_to_excel_multisheet(sheets_counts, out_counts_xlsx)
        else:
            save_to_excel_multisheet(sheets_counts, out_counts_xlsx)
        if not args.no_pickle:
            save_to_pickle(sheets_counts, out_counts_xlsx.with_suffix(".pkl"))

        # 2) Aggregated (optional)
        if args.aggregate:
            sheets_agg = aggregate_employment_regions(dict(sheets_counts))
            sheets_agg = aggregate_company_size(sheets_agg)
            out_agg_xlsx = base_for_kind.with_name(f"{base_for_kind.name}_aggregated").with_suffix(".xlsx")
            save_to_excel_multisheet(sheets_agg, out_agg_xlsx)
            if not args.no_pickle:
                save_to_pickle(sheets_agg, out_agg_xlsx.with_suffix(".pkl"))
        else:
            sheets_agg = None

        # 3) Percent (optional)
        if args.percent:
            source_for_percent = sheets_agg if sheets_agg is not None else sheets_counts
            sheets_pct = convert_all_to_percentages(source_for_percent)
            out_pct_xlsx = base_for_kind.with_name(f"{base_for_kind.name}_percent").with_suffix(".xlsx")
            save_to_excel_multisheet(sheets_pct, out_pct_xlsx)
            if not args.no_pickle:
                save_to_pickle(sheets_pct, out_pct_xlsx.with_suffix(".pkl"))

        # Final consolidated (keep existing behavior for the main output path without suffix)
        # Only produce if not running 'all' to avoid duplication
        if args.analysis != "all":
            out_main_xlsx = base.with_suffix(".xlsx")
            main_sheets = sheets_counts
            if args.aggregate:
                main_sheets = sheets_agg or main_sheets
            if args.percent:
                main_sheets = convert_all_to_percentages(main_sheets)
            if kind == "global" and args.single_sheet:
                save_to_excel_single_report(main_sheets, out_main_xlsx)
            else:
                save_to_excel_multisheet(main_sheets, out_main_xlsx)
            if not args.no_pickle:
                save_to_pickle(main_sheets, out_main_xlsx.with_suffix(".pkl"))

    if args.analysis == "all":
        for kind, sheets_counts in outputs.items():
            save_all_steps(kind, base_out, sheets_counts)
    else:
        save_all_steps(kinds[0], base_out, outputs[kinds[0]])


if __name__ == "__main__":
    main()


