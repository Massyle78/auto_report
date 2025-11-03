import pandas as pd
import pickle
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def convert_to_percentages(sheets_dict):
    """
    Convert aggregated data to percentages.
    
    Args:
        sheets_dict (dict): Dictionary containing aggregated DataFrames
        
    Returns:
        dict: Dictionary containing percentage DataFrames
    """
    percent_sheets = {}
    
    for name, df in sheets_dict.items():
        df = df.copy()
        
        # Find numeric columns (handles MultiIndex as well)
        numeric_cols = df.select_dtypes(include=["number"]).columns
        
        if len(numeric_cols) == 0:
            # No numeric columns -> copy as is
            percent_sheets[name] = df
            continue
        
        # Sum by column
        col_sums = df[numeric_cols].sum(axis=0)
        
        # Avoid division by zero: if sum == 0, set 0 for all values in that column
        # Calculate %: value / col_sum * 100
        # .div handles alignment on column index for MultiIndex as well
        percent_numeric = df[numeric_cols].div(col_sums).multiply(100)
        
        # Columns with sum 0 -> replace NaN/inf with 0
        zero_cols = col_sums[col_sums == 0].index
        if len(zero_cols) > 0:
            percent_numeric.loc[:, zero_cols] = 0.0
        
        # Re-inject non-numeric columns in the same order as original
        # Build final DataFrame respecting original column order
        result = pd.DataFrame(index=df.index)
        for col in df.columns:
            if col in numeric_cols:
                result[col] = percent_numeric[col]
            else:
                result[col] = df[col]
        
        # Keep same dtype of index / names etc.
        percent_sheets[name] = result
    
    return percent_sheets

def save_percentages_to_excel(percent_sheets, output_path):
    """
    Save percentage data to Excel file.
    
    Args:
        percent_sheets (dict): Dictionary containing percentage DataFrames
        output_path (str): Path for the output Excel file
    """
    from openpyxl.utils import get_column_letter
    
    logger.info(f"Saving percentage data to {output_path}")
    
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for name, df in percent_sheets.items():
            # By default, keep the index (like for the original)
            df.to_excel(writer, sheet_name=name)
            
            # Auto-fit columns
            ws = writer.book[name]
            df_reset = df.reset_index() if df.index.names != [None] else df
            for idx, column in enumerate(df_reset.columns, start=1):
                column_letter = get_column_letter(idx)
                values = df_reset[column].astype(str).tolist()
                max_len = max([len(column)] + [len(v) for v in values]) if values else len(column)
                ws.column_dimensions[column_letter].width = max_len + 2
    
    logger.info("Percentage Excel file saved successfully!")

def main():
    """Main function to convert aggregated data to percentages."""
    
    # Input and output paths
    input_pickle = r"C:\Users\Massyle\Documents\auto_report\Enq_TCD_20251020_aggregated.pkl"
    output_excel = r"C:\Users\Massyle\Documents\auto_report\Enq_TCD_20251020_aggregated_percent.xlsx"
    output_pickle = r"C:\Users\Massyle\Documents\auto_report\Enq_TCD_20251020_aggregated_percent.pkl"
    
    try:
        # Load aggregated data
        logger.info(f"Loading aggregated data from {input_pickle}")
        with open(input_pickle, "rb") as f:
            aggregated_sheets = pickle.load(f)
        
        # Convert to percentages
        logger.info("Converting to percentages...")
        percent_sheets = convert_to_percentages(aggregated_sheets)
        
        # Save percentages to Excel
        save_percentages_to_excel(percent_sheets, output_excel)
        
        # Save percentages to pickle
        logger.info(f"Saving percentage data to {output_pickle}")
        with open(output_pickle, "wb") as f:
            pickle.dump(percent_sheets, f)
        
        print("\n" + "="*60)
        print("PERCENTAGE CONVERSION SUMMARY")
        print("="*60)
        print(f"Percentage Excel saved to: {output_excel}")
        print(f"Percentage pickle saved to: {output_pickle}")
        
        # Show sample of converted data
        print("\nSample of EmploiLieuRegionEtranger percentages:")
        print(percent_sheets['EmploiLieuRegionEtranger'].round(2))
        
        print("\nSample of EmploiEntrepriseTaille percentages:")
        print(percent_sheets['EmploiEntrepriseTaille'].round(2))
        
    except Exception as e:
        logger.error(f"Error during percentage conversion: {str(e)}")
        raise

if __name__ == "__main__":
    main()

