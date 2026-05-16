"""
patch_notebook.py
=================
Surgically patches the DeepMind reference notebook with our specific
GCS paths and parameters. Using JSON parsing is much safer than sed
for modifying .ipynb files.
"""

import json
import sys
import os

def patch_notebook(nb_path, target_date):
    if not os.path.exists(nb_path):
        print(f"Error: {nb_path} not found.")
        sys.exit(1)

    print(f"Patching {nb_path} for date {target_date}...")
    
    with open(nb_path, 'r') as f:
        nb = json.load(f)

    # 1. Parameter Replacements
    replacements = {
        'MODEL_PATH = ""': 'MODEL_PATH = "GenCast 0p25deg Operational <2022.npz"',
        'DATA_PATH = ""': f'DATA_PATH = "source-era5_date-{target_date}_res-0.25_levels-13.nc"',
        'STATS_DIR = ""': 'STATS_DIR = "/mnt/gcs_mount_point/stats/"',
        'num_ensemble_members = 8': 'num_ensemble_members = 50'
    }

    # 2. Logic Bypasses (Bypass specific CALLS, not DEFINITIONS)
    bypasses = [
        'assert data_valid_for_model',
        'plot_data(data',
        'display.display(plot_data',
        'display.display(animation',
        'animation.FuncAnimation'
    ]

    found_flags = {k: False for k in list(replacements.keys()) + bypasses}

    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            new_source = []
            for line in cell['source']:
                modified_line = line
                
                # Apply replacements
                for target, replacement in replacements.items():
                    if target in line:
                        modified_line = line.replace(target, replacement)
                        found_flags[target] = True
                
                # Apply bypasses (prefix with #, only if it's a call/assertion)
                for target in bypasses:
                    if target in line and 'def ' not in line:
                        modified_line = f"# {modified_line}"
                        found_flags[target] = True
                        
                new_source.append(modified_line)
            cell['source'] = new_source

    print("Patching results:")
    for target, found in found_flags.items():
        status = "[OK]" if found else "[WARN]"
        print(f"  {status} Found/Patched: {target}")

    with open(nb_path, 'w') as f:
        json.dump(nb, f, indent=1)
    
    print("Notebook patched successfully.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 patch_notebook.py <path_to_ipynb> <target_date>")
        sys.exit(1)
    
    patch_notebook(sys.argv[1], sys.argv[2])
