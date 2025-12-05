import os
import sys
import logging
import pandas as pd
import numpy as np
from tqdm import tqdm
from pydantic import BaseModel, Field, model_validator, ValidationError, ConfigDict

# ==========================================
# 1. CONFIGURATION (ΡΥΘΜΙΣΕΙΣ)
# ==========================================
class ProjectConfig(BaseModel):
    # Επιτρέπει αυθαίρετους τύπους (όπως pandas DataFrame αν χρειαζόταν)
    model_config = ConfigDict(arbitrary_types_allowed=True)

    # DYNAMIC PATHWAYS 
    _src_dir: str = os.path.dirname(os.path.abspath(__file__))
    base_dir: str = os.path.dirname(_src_dir)

    # Παράμετροι Ανάγνωσης
    skip_lines: int = Field(default=8, ge=0, description="Lines to skip in TXT files")
    data_col_index: int = Field(default=1, ge=0, description="Column index for Sample data")
    separator: str = Field(default=';', min_length=1, max_length=1)
    file_extension: str = '.txt'
    folder_name_delimiter: str = '_'
    default_unknown_value: str = "Unknown"
    
    # Helper properties for Paths
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

# ==========================================
# 2. MODELS & VALIDATION (ΕΛΕΓΧΟΙ)
# ==========================================

class SampleMetadata(BaseModel):
    """
    Check if name is valid and put labels.
    """
    original_folder_name: str
    sample_code: str = None
    botanical: str = None
    geographic: str = None

    @model_validator(mode='after')
    def parse_and_validate(self):
        name = self.original_folder_name
        
        # Check 1 : exactly  6 underscores
        if name.count('_') != 6:
            raise ValueError(f"Structure Error: Found {name.count('_')} underscores, expected 6.")
            
        parts = name.split('_')
        # Remove the last part ( empty string due to trailing underscore)
        components = parts[:-1]
        
        # Check  2: 6 components 
        if len(components) != 6:
            raise ValueError(f"Structure Error: Found {len(components)} components, expected 6.")
            
        #Check 3 : First component is not empty 
        if not components[0]:  
            raise ValueError(f"Format Error: First component '{components[0]}' must start with a digit.")
            
        
        # Αν όλα πήγαν καλά, αποθηκεύουμε τις τιμές
        self.sample_code = components[0]
        self.botanical = components[1]
        self.geographic = components[2]
        
        
        return self

class SpectrumFile(BaseModel):
    """
    model that validates and reads a spectrum file.
    """
    filename: str
    folder_path: str
    config: ProjectConfig

    @model_validator(mode='after')
    def validate_file(self):
        fname = self.filename
        cfg = self.config
        
        # Αγνοούμε αρχεία σφαλμάτων
        if "error" in fname.lower():
            raise ValueError(f"Skipping error file: {fname}")
            
        # Ελέγχουμε την κατάληξη
        if not fname.lower().endswith(cfg.file_extension.lower()):
            raise ValueError(f"Skipping non-txt file: {fname}")
            
        return self

    def read_data(self) -> tuple[np.ndarray, np.ndarray]:
        """Reads file and returns  (wavelengths, values)"""
        full_path = os.path.join(self.folder_path, self.filename)
        
        try:
            df = pd.read_csv(
                full_path, 
                sep=self.config.separator, 
                skiprows=self.config.skip_lines, 
                header=None, 
                engine='python', 
                encoding='latin-1', 
                usecols=[0, self.config.data_col_index]
            )
            df = df.dropna()
            
            wavelengths = df[0].values
            values = df[self.config.data_col_index].values
            
            return wavelengths, values
            
        except Exception as e:
            raise RuntimeError(f"Corrupted file or parse error: {e}")

# ==========================================
# 3. SETUP LOGGING
# ==========================================
config = ProjectConfig()
os.makedirs(config.log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG, # CHANGE TO INFO FOR LESS  OUTPUT ??
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout), 
        logging.FileHandler(config.log_file, mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 4. MAIN PROCESS
# ==========================================
def create_dataset(cfg: ProjectConfig):
    data_rows = []
    feature_names = None                    
    primary_key = 1
    
    logger.info("Initializing Dataset Creation Pipeline...")
    logger.info(f"Target Directory: {cfg.raw_data_path}")

    if not os.path.exists(cfg.raw_data_path):
        logger.error(f"CRITICAL: Data directory not found at {cfg.raw_data_path}")
        return

    # Εύρεση όλων των υποφακέλων
    raw_folders = [d for d in os.listdir(cfg.raw_data_path) if os.path.isdir(os.path.join(cfg.raw_data_path, d))]
    
    if not raw_folders:
        logger.warning("No folders found in raw data directory.")
        return

    logger.info(f"Found {len(raw_folders)} folders. Starting processing...")

    # Κύριο Loop ανά φάκελο
    for folder_name in tqdm(raw_folders, desc="Processing Samples"):
        
        # --- VALIDATION ΦΑΚΕΛΟΥ ---
        try:
            meta = SampleMetadata(original_folder_name=folder_name)
        except ValidationError as e:
            # Παίρνουμε το μήνυμα λάθους για το log και συνεχίζουμε στον επόμενο φάκελο
            error_msg = e.errors()[0]['msg'] if e.errors() else str(e)
            logger.warning(f"SKIP FOLDER '{folder_name}': {error_msg}")
            continue
        # --------------------------

        class_folder = os.path.join(cfg.raw_data_path, folder_name)
        files = os.listdir(class_folder)

        # Loop ανά αρχείο 
        for filename in files:
            try:
                # --- VALIDATION & READING ΑΡΧΕΙΟΥ ---
                # Προσπάθεια δημιουργίας αντικειμένου 
                spec_file = SpectrumFile(filename=filename, folder_path=class_folder, config=cfg)
                
                # Ανάγνωση δεδομένων
                wavelengths, values = spec_file.read_data()
                # ------------------------------------

                # Την πρώτη φορά ορίζουμε τα ονόματα των στηλών
                if feature_names is None:
                    feature_names = [f"wl_{w:.3f}" for w in wavelengths]
                    logger.info(f"Features detected. Wavelength range: {wavelengths[0]} - {wavelengths[-1]}")

                # Έλεγχος διαστάσεων
                if len(values) != len(feature_names):
                    logger.debug(f"Skipping {filename}: Dimension mismatch ({len(values)} vs {len(feature_names)})")
                    continue
                
                # Δημιουργία εγγραφής
                row = {
                    'id': primary_key,
                    'sample_code': meta.sample_code,
                    'botanical': meta.botanical,
                    'geographic': meta.geographic,
                    'folder_name': folder_name,
                    'filename': filename
                }
                # Προσθήκη φασματικών δεδομένων
                row.update(dict(zip(feature_names, values)))
                
                data_rows.append(row)
                primary_key += 1

            except (ValidationError, ValueError) as e:
                # Εδώ "πέφτουν" αθόρυβα τα αρχεία που δεν μας ενδιαφέρουν ή τα λάθη validation
                continue
            except Exception as e:
                logger.error(f"Unexpected error processing {filename}: {e}")

    # Αποθήκευση αποτελεσμάτων
    if data_rows:
        logger.info(f"Constructing final DataFrame with {len(data_rows)} samples...")
        final_df = pd.DataFrame(data_rows)
        
        # Τακτοποίηση στηλών
        metadata_cols = ['id', 'sample_code', 'botanical', 'geographic', 'folder_name', 'filename']
        wl_cols = [c for c in final_df.columns if c.startswith('wl_')]
        final_df = final_df[metadata_cols + wl_cols]
        
        os.makedirs(os.path.dirname(cfg.output_path), exist_ok=True)
        final_df.to_csv(cfg.output_path, index=False)
        
        print("\n" + "="*50)
        logger.info("PROCESS COMPLETED SUCCESSFULLY")
        logger.info(f"Dataset saved at: {cfg.output_path}")
        logger.info(f"Shape: {final_df.shape} (Rows, Columns)")
        print("="*50 + "\n")
    else:
        logger.warning("Process finished but no valid data rows were generated.")

if __name__ == "__main__":
    create_dataset(config)