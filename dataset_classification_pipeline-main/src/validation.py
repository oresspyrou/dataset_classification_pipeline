from pydantic import BaseModel, Field, field_validator, ConfigDict
import logging
import re
import numpy as np
from dataclasses import dataclass
import pandas as pd
from config import config, ProjectConfig

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    is_valid: bool
    message: str = "OK"

class SpectralValidator:
    def __init__(self, cfg: ProjectConfig):
        self.cfg = cfg
        self.reference_wavelengths = None  # Εδώ θα αποθηκεύσουμε τον "Χάρακα" (τον άξονα Χ)

    def validate_folder_name(self, folder_name: str) -> ValidationResult:
        if not re.match(self.cfg.foldername_pattern, folder_name, re.IGNORECASE):
            return ValidationResult(False, f"Folder name '{folder_name}' does not match pattern")
        return ValidationResult(True)

    def validate_filename(self, filename: str) -> ValidationResult:
        if "Error" in filename or "error" in filename:
            return ValidationResult(False, "Filename contains 'Error'")
        
        if not filename.lower().endswith(self.cfg.file_extension.lower()):
            return ValidationResult(False, "Wrong extension")
            
        return ValidationResult(True)

    def validate_structure(self, df: pd.DataFrame) -> ValidationResult:
        if df.empty:
            return ValidationResult(False, "Empty DataFrame")
        
        if df.shape[1] < 2: 
            return ValidationResult(
                False, 
                f"Insufficient columns: Found {df.shape[1]}, expected at least 2 (Wave & Value)"
            )
            
        return ValidationResult(True)

    def validate_consistency(self, wavelengths: np.ndarray) -> ValidationResult:
        """
        Ελέγχει αν τα μήκη κύματος (features) είναι ΤΑΥΤΟΣΗΜΑ με το πρώτο αρχείο.
        """
        num_values = len(wavelengths)
        
        if num_values <= 0:
            return ValidationResult(False, f"Empty values: {num_values}")

        # Αν είναι το πρώτο αρχείο που βλέπουμε, το ορίζουμε ως Πρότυπο
        if self.reference_wavelengths is None:
            self.reference_wavelengths = wavelengths
            return ValidationResult(True)
        
        # Έλεγχος 1: Ίδιο Πλήθος σημείων
        if num_values != len(self.reference_wavelengths):
            return ValidationResult(False, f"Length Mismatch: Found {num_values}, expected {len(self.reference_wavelengths)}")
        
        # Έλεγχος 2: Ίδιες Τιμές (με ανοχή 0.01 nm)
        if not np.allclose(wavelengths, self.reference_wavelengths, atol=1e-2):
            diff = np.abs(wavelengths - self.reference_wavelengths).max()
            return ValidationResult(False, f"Feature Mismatch: Wavelengths differ by max {diff:.4f} nm")
        
        return ValidationResult(True)

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
            raise ValueError("Missing metadata")
        return v.strip()


