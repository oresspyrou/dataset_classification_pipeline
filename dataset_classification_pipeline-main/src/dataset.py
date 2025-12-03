import os
import pandas as pd
import numpy as np
import logging
import sys
from tqdm import tqdm
from dataclasses import dataclass

# ==========================================
# 1. CONFIGURATION
# ==========================================
@dataclass
class ProjectConfig:
    _src_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir: str = os.path.dirname(_src_dir)
    
    @property
    def raw_data_path(self):
        return os.path.join(self.base_dir, 'data', 'raw')
    
    @property
    def output_path(self):
        return os.path.join(self.base_dir, 'data', 'processed', 'spectral_dataset_enriched.csv')
    
    @property
    def log_dir(self):
        return os.path.join(self.base_dir, 'logs')
    
    @property
    def log_file(self):
        return os.path.join(self.log_dir, 'dataset_creation_pipeline.log')

    skip_lines: int = 8
    data_col_index: int = 3
    separator: str = ';'
    file_extension: str = '.txt'

config = ProjectConfig()

# ==========================================
# 2. LOGGER SETUP
# ==========================================
os.makedirs(config.log_dir, exist_ok=True) 

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout), 
        logging.FileHandler(config.log_file, mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)    

# ==========================================
# 3. MAIN ETL FUNCTION
# ==========================================
def create_dataset(cfg: ProjectConfig):
    data_rows = []
    feature_names = None
    
    # Αυτό θα είναι το μοναδικό αριθμητικό κλειδί για κάθε μέτρηση (Primary Key)
    global_id_counter = 1
    
    logger.info("Starting ENRICHED dataset creation...")
    logger.info(f"Target Directory: {cfg.raw_data_path}")

    if not os.path.exists(cfg.raw_data_path):
        logger.error(f"CRITICAL: The data wasn't found on: {cfg.raw_data_path}")
        raise FileNotFoundError(f"Missing Data Directory: {cfg.raw_data_path}")
    
    classes = [d for d in os.listdir(cfg.raw_data_path) if os.path.isdir(os.path.join(cfg.raw_data_path, d))]
    
    if not classes:
        logger.warning(f"The folder {cfg.raw_data_path} is empty.")
        return False
    
    logger.info(f"Number of sample folders found: {len(classes)}. Beginning processing...")

    for folder_name in tqdm(classes, desc="Processing Samples", unit="folder"):
        class_folder = os.path.join(cfg.raw_data_path, folder_name)
        files = os.listdir(class_folder)
        
        # --- METADATA PARSING ---
        # Προσπαθούμε να σπάσουμε το όνομα του φακέλου: "0002a_elato_argolida_..."
        parts = folder_name.split('_')
        
        # Default τιμές αν το όνομα δεν ακολουθεί το πρότυπο
        sample_code = folder_name
        botanical_origin = "Unknown"
        geographical_origin = "Unknown"
        
        # Αν το όνομα έχει τουλάχιστον 3 μέρη (π.χ. Code_Bot_Geo_...)
        if len(parts) >= 3:
            sample_code = parts[0]        # 0002a
            botanical_origin = parts[1]   # elato
            geographical_origin = parts[2] # argolida
        
        # ------------------------

        processed_count = 0

        for filename in files:
            if "Error" in filename or "error" in filename:
                continue

            if filename.lower().endswith(cfg.file_extension.lower()):
                file_path = os.path.join(class_folder, filename)
                
                try: 
                    df = pd.read_csv(
                        file_path, 
                        sep=cfg.separator, 
                        skiprows=cfg.skip_lines, 
                        header=None, 
                        engine='python', 
                        encoding='latin-1',
                        usecols=[0, cfg.data_col_index]
                    ) 
                    
                    df = df.dropna()
                                             
                    wavelengths = df[0].values
                    values = df[cfg.data_col_index].values
                    
                    if feature_names is None:
                        feature_names = [f"wl_{w:.3f}" for w in wavelengths]
                        logger.info(f"✅ Setup complete. Features detected.")

                    if len(values) != len(feature_names):
                        continue

                    # Δημιουργία της εγγραφής με τα νέα πεδία
                    row = {
                        'id': global_id_counter,           # 1. Μοναδικό Νούμερο (Key)
                        'sample_code': sample_code,        # 2. Κωδικός Δείγματος (π.χ. 0002a)
                        'botanical': botanical_origin,     # 3. Βοτανική (π.χ. elato)
                        'geographic': geographical_origin, # 4. Γεωγραφική (π.χ. argolida)
                        'folder_name': folder_name,        # Κρατάμε και το αρχικό για reference
                        'filename': filename
                    }
                    
                    # Προσθήκη των φασματικών δεδομένων
                    row.update(dict(zip(feature_names, values)))
                    
                    data_rows.append(row)
                    
                    # Αύξηση μετρητών
                    processed_count += 1
                    global_id_counter += 1
                    
                except Exception as e:
                    logger.warning(f"❌ Failed to read: {filename}. Cause: {e}")

    # ==========================================
    # 4. EXPORT
    # ==========================================
    if data_rows:
        print("\n") 
        logger.info(f"Creating final dataframe with {len(data_rows)} records...")
        final_df = pd.DataFrame(data_rows)
        
        # Οργάνωση στηλών: Τα Metadata μπροστά, τα Wavelengths μετά
        metadata_cols = ['id', 'sample_code', 'botanical', 'geographic', 'folder_name', 'filename']
        # Βεβαιωνόμαστε ότι παίρνουμε τις στήλες wl_ με τη σωστή σειρά
        wl_cols = [c for c in final_df.columns if c.startswith('wl_')]
        
        final_df = final_df[metadata_cols + wl_cols]
        
        os.makedirs(os.path.dirname(cfg.output_path), exist_ok=True)
        
        final_df.to_csv(cfg.output_path, index=False)
        
        logger.info("------------------------------------------------")
        logger.info("PROCESS COMPLETED SUCCESSFULLY")
        logger.info(f"Dataset saved at: {cfg.output_path}")
        logger.info(f"Dimensions: {final_df.shape} (Rows, Columns)")
        logger.info("Sample of extracted metadata:")
        logger.info(final_df[['id', 'botanical', 'geographic']].head(3))
    else:
        logger.warning("⚠️ The list of data rows is empty.")

if __name__ == "__main__":
    create_dataset(config)