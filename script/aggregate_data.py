import pandas as pd
import pickle
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def aggregate_data(pickle_path):
    """
    Aggregate specific categories in the data before converting to percentages.
    
    Args:
        pickle_path (str): Path to the original pickle file
        
    Returns:
        dict: Aggregated data dictionary
    """
    
    # Load the original data
    logger.info(f"Loading data from {pickle_path}")
    with open(pickle_path, "rb") as f:
        sheets_dict = pickle.load(f)
    
    # Create a copy for aggregation
    aggregated_sheets = sheets_dict.copy()
    
    # 1. Aggregate EmploiLieuRegionEtranger
    if 'EmploiLieuRegionEtranger' in aggregated_sheets:
        logger.info("Aggregating EmploiLieuRegionEtranger...")
        df_region = aggregated_sheets['EmploiLieuRegionEtranger'].copy()
        
        # Define regions to keep as is
        regions_to_keep = ['Île-de-France', 'Étranger']
        
        # Get all other regions (to be summed into 'Province')
        other_regions = [region for region in df_region.index if region not in regions_to_keep]
        
        if other_regions:
            # Sum all other regions into 'Province'
            province_data = df_region.loc[other_regions].sum()
            
            # Create new dataframe with aggregated data
            # Keep the original regions that we want to preserve
            kept_data = df_region.loc[regions_to_keep]
            
            # Add the new 'Province' row
            province_df = pd.DataFrame([province_data], index=['Province'])
            
            # Combine kept regions and province
            aggregated_region = pd.concat([kept_data, province_df])
            
            # Sort index for better readability
            aggregated_region = aggregated_region.sort_index()
            
            aggregated_sheets['EmploiLieuRegionEtranger'] = aggregated_region
            
            logger.info(f"Regions aggregated: {len(other_regions)} regions combined into 'Province'")
            logger.info(f"Final regions: {list(aggregated_region.index)}")
    
    # 2. Aggregate EmploiEntrepriseTaille
    if 'EmploiEntrepriseTaille' in aggregated_sheets:
        logger.info("Aggregating EmploiEntrepriseTaille...")
        df_taille = aggregated_sheets['EmploiEntrepriseTaille'].copy()
        
        # Define categories to sum into 'Moins de 10'
        categories_to_sum = ['0', 'De 1 à 9']
        
        # Check if both categories exist
        existing_categories = [cat for cat in categories_to_sum if cat in df_taille.index]
        
        if len(existing_categories) > 0:
            # Sum the specified categories
            moins_de_10_data = df_taille.loc[existing_categories].sum()
            
            # Create new dataframe without the categories we're aggregating
            other_categories = [cat for cat in df_taille.index if cat not in categories_to_sum]
            kept_data = df_taille.loc[other_categories]
            
            # Add the new 'Moins de 10' row
            moins_de_10_df = pd.DataFrame([moins_de_10_data], index=['Moins de 10'])
            
            # Combine kept categories and new aggregated category
            aggregated_taille = pd.concat([kept_data, moins_de_10_df])
            
            # Sort index for better readability
            aggregated_taille = aggregated_taille.sort_index()
            
            aggregated_sheets['EmploiEntrepriseTaille'] = aggregated_taille
            
            logger.info(f"Categories aggregated: {existing_categories} combined into 'Moins de 10'")
            logger.info(f"Final categories: {list(aggregated_taille.index)}")
    
    return aggregated_sheets

def save_aggregated_data(aggregated_sheets, output_path):
    """
    Save the aggregated data to a new pickle file.
    
    Args:
        aggregated_sheets (dict): Aggregated data dictionary
        output_path (str): Path for the output pickle file
    """
    logger.info(f"Saving aggregated data to {output_path}")
    with open(output_path, "wb") as f:
        pickle.dump(aggregated_sheets, f)
    
    logger.info("Aggregated data saved successfully!")

def main():
    """Main function to run the aggregation process."""
    
    # Input and output paths
    input_pickle = r"C:\Users\Massyle\Documents\auto_report\Enq_TCD_20251020.pkl"
    output_pickle = r"C:\Users\Massyle\Documents\auto_report\Enq_TCD_20251020_aggregated.pkl"
    
    try:
        # Perform aggregation
        aggregated_data = aggregate_data(input_pickle)
        
        # Save aggregated data
        save_aggregated_data(aggregated_data, output_pickle)
        
        # Display summary of changes
        print("\n" + "="*50)
        print("AGGREGATION SUMMARY")
        print("="*50)
        
        # Show EmploiLieuRegionEtranger changes
        if 'EmploiLieuRegionEtranger' in aggregated_data:
            print("\nEmploiLieuRegionEtranger:")
            print("Final regions:", list(aggregated_data['EmploiLieuRegionEtranger'].index))
        
        # Show EmploiEntrepriseTaille changes
        if 'EmploiEntrepriseTaille' in aggregated_data:
            print("\nEmploiEntrepriseTaille:")
            print("Final categories:", list(aggregated_data['EmploiEntrepriseTaille'].index))
        
        print(f"\nAggregated data saved to: {output_pickle}")
        
    except Exception as e:
        logger.error(f"Error during aggregation: {str(e)}")
        raise

if __name__ == "__main__":
    main()

