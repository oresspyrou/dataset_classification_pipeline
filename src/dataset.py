import os
import pandas as pd
import numpy as np
from datetime import datetime

# ==========================================
# ΡΥΘΜΙΣΕΙΣ (PATHS)
# ==========================================
# Βρίσκουμε αυτόματα το path, υποθέτοντας ότι τρέχουμε το script από το root του project
BASE_DIR = os.getcwd() 
RAW_DATA_PATH = os.path.join(BASE_DIR, 'data', 'raw')
OUTPUT_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'spectral_dataset.csv')

# Ρυθμίσεις αρχείων
SKIP_LINES = 8 
DATA_COL_INDEX = 3  # Η στήλη με το Scope/Absorbance

# ==========================================
# ΛΟΓΙΚΗ ΕΠΕΞΕΡΓΑΣΙΑΣ
# ==========================================

def create_dataset():
    data_rows = []
    feature_names = None
    
    print(f"🚀 Έναρξη διαδικασίας...")
    print(f"📂 Ανάγνωση από: {RAW_DATA_PATH}")

    # Έλεγχος αν υπάρχει ο φάκελος
    if not os.path.exists(RAW_DATA_PATH):
        print(f"❌ ΣΦΑΛΜΑ: Δεν βρέθηκε ο φάκελος {RAW_DATA_PATH}")
        print("   Βεβαιώσου ότι έβαλες τα δεδομένα στο 'data/raw/'")
        return

    # Βρίσκουμε τις κλάσεις (τους φακέλους)
    classes = [d for d in os.listdir(RAW_DATA_PATH) if os.path.isdir(os.path.join(RAW_DATA_PATH, d))]
    
    if not classes:
        print("❌ Δεν βρέθηκαν φάκελοι δεδομένων!")
        return

    print(f"   Βρέθηκαν {len(classes)} κλάσεις (φάκελοι).")

    for class_name in classes:
        class_folder = os.path.join(RAW_DATA_PATH, class_name)
        files = os.listdir(class_folder)
        
        # Μετρητής για να βλέπουμε πρόοδο
        processed_count = 0
        
        for filename in files:
            if filename.endswith(".txt"):
                file_path = os.path.join(class_folder, filename)
                
                try:
                    # Διάβασμα αρχείου
                    df = pd.read_csv(file_path, 
                                     sep=';', 
                                     skiprows=SKIP_LINES, 
                                     header=None, 
                                     engine='python',
                                     usecols=[0, DATA_COL_INDEX]) 
                    
                    wavelengths = df[0].values
                    values = df[DATA_COL_INDEX].values
                    
                    # Ορισμός Επικεφαλίδων (μόνο την πρώτη φορά)
                    if feature_names is None:
                        feature_names = [f"wl_{w:.3f}" for w in wavelengths]
                        print(f"ℹ️  Διαστάσεις φάσματος: {len(feature_names)} σημεία.")

                    # Έλεγχος εγκυρότητας
                    if len(values) != len(feature_names):
                        continue # Skip bad files

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
                    print(f"⚠️ Error in {filename}: {e}")
        
        print(f"   ✅ {class_name}: Επεξεργάστηκαν {processed_count} αρχεία.")

    # ==========================================
    # ΑΠΟΘΗΚΕΥΣΗ
    # ==========================================
    if data_rows:
        print("💾 Δημιουργία DataFrame...")
        final_df = pd.DataFrame(data_rows)
        
        # Τακτοποίηση στηλών
        cols = ['measurement_id', 'label', 'filename'] + [c for c in final_df.columns if c not in ['measurement_id', 'label', 'filename']]
        final_df = final_df[cols]
        
        # Δημιουργία φακέλου processed αν δεν υπάρχει
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        
        final_df.to_csv(OUTPUT_PATH, index=False)
        print("------------------------------------------------")
        print(f"🎉 ΕΠΙΤΥΧΙΑ! Το Dataset δημιουργήθηκε.")
        print(f"📍 Αποθηκεύτηκε στο: {OUTPUT_PATH}")
        print(f"📊 Μέγεθος: {final_df.shape}")
    else:
        print("⚠️ Δεν βρέθηκαν δεδομένα για αποθήκευση.")

if __name__ == "__main__":
    create_dataset()