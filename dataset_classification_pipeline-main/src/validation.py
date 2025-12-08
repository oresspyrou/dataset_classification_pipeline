from pydantic import BaseModel, Field, field_validator, ConfigDict
import logging
from src.config import ProjectConfig
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    is_valid: bool
    message: str = ""

class SpectralValidator:
    
    def __init__(self, cfg: ProjectConfig):
        self.cfg = cfg
    
    def validate_folder_name(self, folder_name: str) -> ValidationResult:
        if not folder_name or not isinstance(folder_name, str):
            return ValidationResult(is_valid=False, message="Invalid folder name type")
        
        if not re.match(self.cfg.foldername_pattern, folder_name, re.IGNORECASE):
            return ValidationResult(
                is_valid=False, 
                message=f"Folder name '{folder_name}' does not match pattern"
            )
        return ValidationResult(is_valid=True)
    
    def validate_filename(self, filename: str) -> ValidationResult:
        if not filename or not isinstance(filename, str):
            return ValidationResult(is_valid=False, message="Invalid filename type")
        
        if not re.match(self.cfg.filename_pattern, filename, re.IGNORECASE):
            return ValidationResult(
                is_valid=False, 
                message=f"Filename '{filename}' does not match pattern"
            )
        return ValidationResult(is_valid=True)
    
    def validate_structure(self, df) -> ValidationResult:
        if df is None or df.empty:
            return ValidationResult(is_valid=False, message="DataFrame is empty")
        
        if len(df.columns) < 2:
            return ValidationResult(
                is_valid=False, 
                message=f"DataFrame has only {len(df.columns)} columns, expected at least 2"
            )
        return ValidationResult(is_valid=True)
    
    def validate_consistency(self, num_values: int) -> ValidationResult:
        """Επικυρώνει ότι ο αριθμός τιμών είναι συνεπής"""
        if num_values <= 0:
            return ValidationResult(
                is_valid=False, 
                message=f"Invalid number of values: {num_values}"
            )
        return ValidationResult(is_valid=True)

class SpectralRecord(BaseModel):
    """Pydantic model για ένα spectral record με field validators"""
  
    model_config = ConfigDict(extra='forbid')  # Απαγόρευση επιπλέον πεδίων
    
    id: int = Field(gt=0)
    sample_code: str = Field(min_length=1)
    botanical: str 
    geographic: str 
    folder_name: str
    filename: str

    @field_validator('filename')
    @classmethod
    def validate_filename_format(cls, v: str) -> str:
        """Επικυρώνει το format του filename με regex pattern"""
        from src.config import config
        if not re.match(config.filename_pattern, v, re.IGNORECASE):
            raise ValueError(f"Invalid Filename Format: '{v}'")
        return v

    @field_validator('botanical', 'geographic')
    @classmethod
    def clean_strings(cls, v: str) -> str:
       
        if not v or v.strip() == "":
            raise ValueError(f"Missing metadata")
            
        return v.strip()


