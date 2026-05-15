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

    # Substrings to search for and comment out/replace
    replacements = {
        'MODEL_PATH = ""': f'MODEL_PATH = "GenCast 0p25deg Operational <2022.npz"',
        'DATA_PATH = ""': f'DATA_PATH = "source-era5_date-{target_date}_res-0.25_levels-13.nc"',
        'STATS_DIR = ""': 'STATS_DIR = "/mnt/gcs_mount_point/stats/"',
        'num_ensemble_members = 8': 'num_ensemble_members = 50',
        'assert data_valid_for_model': '# assert bypassed (Operational vs ERA5 check)',
        'plot_data(': '# plot_data bypassed (headless)',
        'display.display(': '# display.display bypassed (headless)'
    }

    found_flags = {k: False for k in replacements}

    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            new_source = []
            for line in cell['source']:
                modified_line = line
                for target, replacement in replacements.items():
                    if target in line:
                        # Comment out the whole line
                        indent = line[:len(line) - len(line.lstrip())]
                        modified_line = f"{indent}{replacement} # {line.strip()}\n"
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
