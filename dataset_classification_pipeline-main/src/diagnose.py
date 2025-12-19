import os
import re
import glob
from src.config import config

def diagnose_issues():
    print("--- DIAGNOSTIC TOOL ---")
    
    # 1. Έλεγχος Πλήθους Αρχείων
    raw_path = config.raw_data_path
    print(f"Checking folder: {raw_path}")
    
    if not os.path.exists(raw_path):
        print("ERROR: Raw data path does not exist!")
        return

    subfolders = [f.path for f in os.scandir(raw_path) if f.is_dir()]
    print(f"Found {len(subfolders)} subfolders.")
    
    total_files = 0
    regex = re.compile(config.filename_pattern, re.IGNORECASE)
    
    for folder in subfolders:
        files = os.listdir(folder)
        count = len(files)
        total_files += count
        print(f"\nFolder: {os.path.basename(folder)}")
        print(f"  -> Total files inside: {count}")
        
        # Έλεγχος Regex
        matches = [f for f in files if regex.match(f)]
        print(f"  -> Files matching Regex: {len(matches)}")
        
        if len(matches) < count:
            print("  -> ⚠️ WARNING: Some files are ignored by Regex!")
            # Δείξε μας τα πρώτα 5 που αγνοούνται
            ignored = [f for f in files if not regex.match(f)]
            print(f"     Examples of ignored files: {ignored[:5]}")

    print(f"\nTotal files on disk: {total_files}")
    
    # 2. Έλεγχος του CSV (αν υπάρχει)
    csv_path = config.output_path
    if os.path.exists(csv_path):
        # Μετράμε γραμμές χωρίς να φορτώσουμε όλο το αρχείο (για ταχύτητα)
        with open(csv_path, 'r', encoding='utf-8') as f:
            row_count = sum(1 for _ in f) - 1 # Αφαιρούμε header
        print(f"Total rows in CSV: {row_count}")
        
        if row_count == 9999:
            print("⚠️ ALERT: CSV has exactly 9999 rows. Check ignored files above!")
    else:
        print("CSV file not found.")

if __name__ == "__main__":
    diagnose_issues()