import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from src.config import config

def perform_eda():
    print("--- Starting Exploratory Data Analysis (EDA) ---")
    
    if not os.path.exists(config.output_path):
        print(f"Error: Dataset not found at {config.output_path}. Run dataset.py first.")
        return

    # Φόρτωση του CSV
    print("Loading dataset... (this might take a moment)")
    df = pd.read_csv(config.output_path)
    print(f"Dataset Loaded. Shape: {df.shape}")
    
    spectral_cols = [c for c in df.columns if c.startswith('wl_')]
    
    # 1. Κατανομή Κλάσεων (Μπάρες)
    plt.figure(figsize=(10, 5))
    sns.countplot(data=df, x='botanical', hue='botanical', palette='viridis', legend=False)
    plt.title('Distribution of Botanical Classes')
    plt.show()

    # 2. Φασματική Υπογραφή (Γραμμές)
    print("Plotting Spectral Signatures...")
    plt.figure(figsize=(12, 6))
    
    # Μετατροπή ονομάτων στηλών σε αριθμούς για τον άξονα Χ
    x_axis = [float(col.replace('wl_', '')) for col in spectral_cols]
    
    for label in df['botanical'].unique():
        subset = df[df['botanical'] == label]
        mean_spectrum = subset[spectral_cols].mean(axis=0).values
        plt.plot(x_axis, mean_spectrum, label=label)
        
    plt.title('Mean Spectral Signature by Class')
    plt.xlabel('Wavelength (nm)')
    plt.ylabel('Intensity')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.show()

    # 3. Έλεγχος Outliers (Boxplot)
    df['max_intensity'] = df[spectral_cols].max(axis=1)
    plt.figure(figsize=(10, 5))
    sns.boxplot(data=df, x='botanical', y='max_intensity', hue='botanical', palette='Set2', legend=False)
    plt.title('Max Intensity Check (Outliers)')
    plt.show()
    
    print("\n--- EDA Completed ---")

if __name__ == "__main__":
    perform_eda()