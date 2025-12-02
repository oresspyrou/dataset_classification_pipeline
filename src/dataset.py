import os
import pandas as pd
import numpy as np
import logging
import sys

BASE_DIR = os.getcwd() #root
RAW_DATA_PATH = os.path.join(BASE_DIR, 'data', 'raw')
OUTPUT_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'spectral_dataset.csv')

LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, 'dataset_creation_pipeline.log')

SKIP_LINES = 8 
DATA_COL_INDEX = 3 

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout), 
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

def create_dataset():
    data_rows = []
    feature_names = None
    
    logger.info("Starting dataset creation...")

    if not os.path.exists(RAW_DATA_PATH):
        logger.error(f"CRITICAL: The data wasn't found on: {RAW_DATA_PATH}")

        raise FileNotFoundError(f"Missing Data Directory: {RAW_DATA_PATH}")
    
    classes = [d for d in os.listdir(RAW_DATA_PATH) if os.path.isdir(os.path.join(RAW_DATA_PATH, d))]
    
    if not classes:
        logger.warning(f"The folder {RAW_DATA_PATH}, is empty.")
        logger.warning("No dataset can be produced.")
        
        return False
    
    logger.info(f"Number of classes found: {len(classes)}. Beginning processing...")

    for class_name in classes:
        class_folder = os.path.join(RAW_DATA_PATH, class_name)
        files = os.listdir(class_folder)
        
        processed_count = 0

        for filename in files:
            if filename.endswith(".txt"):
                file_path = os.path.join(class_folder, filename)
                
                try: 
                    df = pd.read_csv(file_path, sep=';', skiprows=SKIP_LINES, header=None, engine='python', usecols=[0, DATA_COL_INDEX]) 
                                      
                    wavelengths = df[0].values
                    values = df[DATA_COL_INDEX].values
                    
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
        
        logger.info(f"Class processed:'{class_name}': {processed_count} files added.")

    if data_rows:
        logger.info(f"Creating final dataframe with {len(data_rows)} records...")
        final_df = pd.DataFrame(data_rows)
        
        # Τακτοποίηση στηλών
        cols = ['measurement_id', 'label', 'filename'] + [c for c in final_df.columns if c not in ['measurement_id', 'label', 'filename']]
        final_df = final_df[cols]
        
        # Δημιουργία φακέλου processed αν δεν υπάρχει
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        
        final_df.to_csv(OUTPUT_PATH, index=False)
        logger.info("------------------------------------------------")
        logger.info("PROCESS COMPLETED SUCCESSFULLY")
        logger.info(f"The dataset is saved here: {OUTPUT_PATH}")
        logger.info(f"Dimentions: {final_df.shape} (Rows, Collumns)")
    else:
        logger.warning("The list of data rows is empty.")
        logger.warning("No .txt files were processed.")
        logger.warning("Check the folder data\raw for the correct file types.")

if __name__ == "__main__":
    create_dataset()