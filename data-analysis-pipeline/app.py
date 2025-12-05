import streamlit as st
import pandas as pd
import sys
from pathlib import Path
import tempfile
import os
import zipfile
import io
import shutil

# Add the current directory to sys.path to make imports work
# This allows running `streamlit run data-analysis-pipeline/app.py` from the root
sys.path.append(str(Path(__file__).parent))

from src.processing.data_loader import get_prepared_data
from src.analysis.global_analysis import run_global_analysis
from src.analysis.branch_analysis import run_branch_analysis
from src.analysis.filiere_analysis import run_filiere_analysis
from src.processing.post_processing import (
    aggregate_company_size,
    aggregate_employment_regions,
    convert_all_to_percentages,
)
from src.io.data_writer import save_to_excel_singlesheet
from config.settings import SUMMARY_COLUMNS

st.set_page_config(page_title="Pipeline d'Analyse de Données", layout="wide")

st.title("Plateforme de Déploiement de Pipeline d'Analyse")

st.markdown("""
Cette application permet de :
1. Charger un fichier Excel d'entrée.
2. Choisir les options de traitement.
3. Générer et télécharger les rapports d'analyse.
""")

# --- Sidebar options ---
st.sidebar.header("Configuration")

uploaded_file = st.sidebar.file_uploader("Choisir un fichier Excel", type=["xlsx", "xls"])

analysis_type = st.sidebar.selectbox(
    "Type d'analyse",
    ["global", "branch", "filiere", "all"],
    index=3,
    help="Choisissez le niveau d'analyse souhaité."
)

do_aggregate = st.sidebar.checkbox(
    "Agréger les données",
    value=True,
    help="Appliquer l'agrégation (taille d'entreprise, régions)."
)

do_percent = st.sidebar.checkbox(
    "Convertir en pourcentages",
    value=True,
    help="Générer des fichiers avec des pourcentages."
)

# --- Main Logic ---

def run_analysis_logic(kind: str, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if kind == "global":
        return run_global_analysis(df, SUMMARY_COLUMNS)
    if kind == "branch":
        return run_branch_analysis(df, SUMMARY_COLUMNS)
    if kind == "filiere":
        return run_filiere_analysis(df, SUMMARY_COLUMNS)
    raise ValueError(f"Unknown analysis kind: {kind}")

if uploaded_file is not None:
    st.info(f"Fichier chargé : {uploaded_file.name}")
    
    if st.button("Lancer l'analyse"):
        with st.spinner('Traitement en cours...'):
            try:
                # Create a temporary directory for processing
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_path = Path(temp_dir)
                    input_path = temp_path / uploaded_file.name
                    
                    # Save uploaded file
                    with open(input_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    
                    # Load data
                    # We pass the temp directory and filename to get_prepared_data
                    # It handles loading, cleaning, and filtering
                    df = get_prepared_data(input_dir=temp_path, input_file_name=uploaded_file.name)
                    
                    outputs = {}
                    kinds = [analysis_type] if analysis_type != "all" else ["global", "branch", "filiere"]
                    
                    generated_files = []

                    for kind in kinds:
                        st.text(f"Exécution de l'analyse : {kind}...")
                        
                        # 1. Counts
                        sheets_counts = run_analysis_logic(kind, df)
                        
                        # Save counts
                        out_counts_name = f"report_{kind}_counts.xlsx"
                        out_counts_path = temp_path / out_counts_name
                        save_to_excel_singlesheet(sheets_counts, out_counts_path)
                        generated_files.append(out_counts_path)
                        
                        sheets_agg = None
                        # 2. Aggregated
                        if do_aggregate:
                            sheets_agg = aggregate_employment_regions(dict(sheets_counts))
                            sheets_agg = aggregate_company_size(sheets_agg)
                            
                            out_agg_name = f"report_{kind}_aggregated.xlsx"
                            out_agg_path = temp_path / out_agg_name
                            save_to_excel_singlesheet(sheets_agg, out_agg_path)
                            generated_files.append(out_agg_path)
                        
                        # 3. Percent
                        if do_percent:
                            source_for_percent = sheets_agg if sheets_agg is not None else sheets_counts
                            sheets_pct = convert_all_to_percentages(source_for_percent)
                            
                            out_pct_name = f"report_{kind}_percent.xlsx"
                            out_pct_path = temp_path / out_pct_name
                            save_to_excel_singlesheet(sheets_pct, out_pct_path)
                            generated_files.append(out_pct_path)

                    st.success("Analyse terminée avec succès !")

                    # Create a zip file containing all generated reports
                    zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                        for file_path in generated_files:
                            zip_file.write(file_path, arcname=file_path.name)
                    
                    st.download_button(
                        label="Télécharger tous les rapports (ZIP)",
                        data=zip_buffer.getvalue(),
                        file_name="rapports_analyse.zip",
                        mime="application/zip"
                    )
                    
                    # Optional: Display preview of some data
                    st.subheader("Aperçu des résultats")
                    if kinds:
                        preview_kind = kinds[0]
                        st.write(f"Aperçu pour {preview_kind} (première feuille) :")
                        # Display the first sheet of the first analysis
                        # We need to re-read or just use the variable we have.
                        # Since we iterated, let's just grab the last computed sheets_counts or similar
                        # But better to just show the last processed one from the loop
                        if 'sheets_counts' in locals():
                             first_sheet_name = list(sheets_counts.keys())[0]
                             st.write(f"Feuille : {first_sheet_name}")
                             st.dataframe(sheets_counts[first_sheet_name].head())


            except Exception as e:
                st.error(f"Une erreur est survenue : {e}")
                # Print traceback to console for debugging
                import traceback
                traceback.print_exc()

else:
    st.info("Veuillez charger un fichier Excel pour commencer.")

