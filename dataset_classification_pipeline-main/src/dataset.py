import os
import pandas as pd
import numpy as np
import logging
import sys
from tqdm import tqdm
from dataclasses import dataclass

@dataclass
class ProjectConfig:

    base_dir: str = os.getcwd()
    
    @property
    def raw_data_path(self):
        return os.path.join(self.base_dir, 'data', 'raw')
    
    @property
    def output_path(self):
        return os.path.join(self.base_dir, 'data', 'processed', 'spectral_dataset.csv')
    
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

def create_dataset(cfg: ProjectConfig):
    data_rows = []
    feature_names = None
    
    logger.info("Starting dataset creation...")
    logger.info(f"Configuration: Skip Lines={cfg.skip_lines}, Col Index={cfg.data_col_index}")

    if not os.path.exists(cfg.raw_data_path):
        logger.error(f"CRITICAL: The data wasn't found on: {cfg.raw_data_path}")

        raise FileNotFoundError(f"Missing Data Directory: {cfg.raw_data_path}")
    
    classes = [d for d in os.listdir(cfg.raw_data_path) if os.path.isdir(os.path.join(cfg.raw_data_path, d))]
    
    if not classes:
        logger.warning(f"The folder {cfg.raw_data_path}, is empty.")
        logger.warning("No dataset can be produced.")
        
        return False
    
    logger.info(f"Number of classes found: {len(classes)}. Beginning processing...")

    for class_name in tqdm(classes, desc="Processing Classes", unit="class"):
        class_folder = os.path.join(cfg.raw_data_path, class_name)
        files = os.listdir(class_folder)
        
        processed_count = 0

        for filename in files:
            if filename.endswith(".txt"):
                file_path = os.path.join(class_folder, filename)
                
                try: 
                    df = pd.read_csv(file_path, sep=';', skiprows=cfg.skip_lines, header=None, engine='python', usecols=[0, cfg.data_col_index]) 
                                      
                    wavelengths = df[0].values
                    values = df[cfg.data_col_index].values
                    
                    if feature_names is None:
                        feature_names = [f"wl_{w:.3f}" for w in wavelengths]

                        logger.info(f"Detected {len(feature_names)} spectral features.")

                    # Έλεγχος εγκυρότητας
                    if len(values) != len(feature_names):
                        logger.warning(f"SKIPPING {filename}: Wrong dimentions: ({len(values)}, should have been:{len(feature_names)})")
                        continue

                    # Δημιουργία ID
                    clean_fname = filename.rsplit('.', 1)[0]
                    unique_id = f"{class_name}_{clean_fname}"
                    
                    # Δημιουργία εγγραφής
                    row = {
                        'measurement_id': unique_id,
                        'label': class_name,
                        'filename': filename
                    }
                    row.update(dict(zip(feature_names, values)))
                    
                    data_rows.append(row)
                    processed_count += 1
                    
                except Exception as e:
                    logger.warning(f"Fail to read: {filename}. Cause: {e}")
        
        logger.debug(f"Class processed '{class_name}': {processed_count} files added.")

    if data_rows:
        logger.info(f"Creating final dataframe with {len(data_rows)} records...")
        final_df = pd.DataFrame(data_rows)
        
        # Τακτοποίηση στηλών
        cols = ['measurement_id', 'label', 'filename'] + [c for c in final_df.columns if c not in ['measurement_id', 'label', 'filename']]
        final_df = final_df[cols]
        
        # Δημιουργία φακέλου processed αν δεν υπάρχει
        os.makedirs(os.path.dirname(cfg.output_path), exist_ok=True)
        
        final_df.to_csv(cfg.output_path, index=False)
        logger.info("------------------------------------------------")
        logger.info("PROCESS COMPLETED SUCCESSFULLY")
        logger.info(f"The dataset is saved here: {cfg.output_path}")
        logger.info(f"Dimentions: {final_df.shape} (Rows, Collumns)")
    else:
        logger.warning("The list of data rows is empty.")
        logger.warning("No .txt files were processed.")
        logger.warning("Check the folder data\raw for the correct file types.")

if __name__ == "__main__":
    create_dataset(config)