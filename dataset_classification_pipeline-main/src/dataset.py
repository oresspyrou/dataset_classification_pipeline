import os
import pandas as pd
import numpy as np
import logging
import sys
from tqdm import tqdm
from dataclasses import dataclass
from pydantic import BaseModel, ConfigDict
import src.config as ProjectConfig

<<<<<<< HEAD
=======
@dataclass
class ProjectConfig:
    _src_dir = os.path.dirname(os.path.abspath(__file__))#ggs
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
>>>>>>> ae6833887912b525384bde7b91048ffe345901b0


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout), 
        logging.FileHandler(config.log_file, mode='a', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)    

def parse_folder_metadata(folder_name: str, cfg: ProjectConfig) -> dict:
   
    parts = folder_name.split(cfg.folder_name_delimiter)
    
    metadata = {
        'sample_code': folder_name,
        'botanical': cfg.default_unknown_value,# So it does not crash
        'geographic': cfg.default_unknown_value
    }
    
    if len(parts) >= 3:
        metadata['sample_code'] = parts[0]
        metadata['botanical'] = parts[1]
        metadata['geographic'] = parts[2]
        
    return metadata

def create_dataset(cfg: ProjectConfig):
    data_rows = []
    feature_names = None                    
    primary_key= 1
    
    logger.info("Starting dataset creation...")
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
        
        meta = parse_folder_metadata(folder_name, cfg)
  
        processed_count = 0

        for filename in files:
            if "Error" in filename or "error" in filename:
                continue

            if filename.lower().endswith(cfg.file_extension.lower()): # This way it understands .TXT in Linux-Python format
                file_path = os.path.join(class_folder, filename)
                
                try: 
                    df = pd.read_csv(file_path, sep=cfg.separator, skiprows=cfg.skip_lines, header=None, engine='python', encoding='latin-1', usecols=[0, cfg.data_col_index]) 
                    
                    df = df.dropna()
                                             
                    wavelengths = df[0].values
                    values = df[cfg.data_col_index].values
                    
                    if feature_names is None:
                        feature_names = [f"wl_{w:.3f}" for w in wavelengths]
                        logger.info(f"Setup complete. Features detected.")

                    if len(values) != len(feature_names):
                        continue
                    
                    row = {
                        'id': primary_key,
                        'sample_code': meta['sample_code'],
                        'botanical': meta['botanical'],
                        'geographic': meta['geographic'],
                        'folder_name': folder_name,
                        'filename': filename
                    }
                    row.update(dict(zip(feature_names, values)))
                    
                    data_rows.append(row)
                    
                    processed_count += 1
                    primary_key += 1
                    
                except Exception as e:
                    logger.debug(f"Failed to read: {filename}. Cause: {e}")

    if data_rows:
        print("\n") 
        logger.info(f"Creating final dataframe with {len(data_rows)} records...")
        final_df = pd.DataFrame(data_rows)
        
        metadata_cols = ['id', 'sample_code', 'botanical', 'geographic', 'folder_name', 'filename']
        wl_cols = [c for c in final_df.columns if c.startswith('wl_')]
        
        final_df = final_df[metadata_cols + wl_cols]
        
        os.makedirs(os.path.dirname(cfg.output_path), exist_ok=True)
        
        final_df.to_csv(cfg.output_path, index=False)
        
        logger.info("------------------------------------------------")
        logger.info("PROCESS COMPLETED SUCCESSFULLY") #GGS
        logger.info(f"Dataset saved at: {cfg.output_path}")
        logger.info(f"Dimensions: {final_df.shape} (Rows, Columns)")
    else:
        logger.warning("The list of data rows is empty.")

if __name__ == "__main__":
    create_dataset(config)