from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Any
import logging
from src.config import config
import re

logger = logging.getLogger(__name__)

class FolderNameValidator:
    
    @staticmethod
    def is_valid(folder_name: str) -> bool:
        
        if not re.match(config.foldername_pattern, folder_name, re.IGNORECASE):
            return False
        return True

class SpectralRecord(BaseModel):
  
    model_config = ConfigDict(extra='allow') 
    
    id: int = Field(gt=0)
    sample_code: str = Field(min_length=1)
    botanical: str 
    geographic: str 
    folder_name: str
    filename: str

    @field_validator('filename')
    @classmethod
    def validate_filename_format(cls, v: str) -> str:
        
        if not re.match(config.filename_pattern, v, re.IGNORECASE):
            raise ValueError(f"Invalid Filename Format: '{v}'")
            
        return v

    @field_validator('botanical', 'geographic')
    @classmethod
    def clean_strings(cls, v: str) -> str:
       
        if not v or v.strip() == "":
            raise ValueError(f"Missing metadata")
            
        return v.strip()


