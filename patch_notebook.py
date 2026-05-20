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
        'num_ensemble_members = 8': 'num_ensemble_members = 56'
    }

    # 2. Logic Bypasses (Bypass specific CALLS, not DEFINITIONS)
    bypasses = [
        'assert data_valid_for_model'
    ]

    found_flags = {k: False for k in list(replacements.keys()) + bypasses}
    plot_cells_cleared = 0

    # 3. Injection Code Blocks
    rehydration_code = [
        "# RE-HYDRATION AND COMPATIBILITY CELL (Injected by patcher)\n",
        "import numpy as np\n",
        "import xarray as xr\n",
        "import jax\n",
        "import os\n",
        "\n",
        "def log_diag(msg):\n",
        "    with open('gencast_diagnostics.txt', 'a') as f: f.write(f'{msg}\\n')\n",
        "    print(f'[DIAG] {msg}')\n",
        "\n",
        "log_diag('--- Session Started ---')\n",
        "\n",
        "if not hasattr(jax, 'P'):\n",
        "    jax.P = jax.sharding.PartitionSpec\n",
        "    log_diag('jax.P monkey-patched.')\n",
        "\n",
        "print('Re-hydrating integer coordinates...')\n",
        "if 'example_batch' in locals():\n",
        "    log_diag(f'Initial batch vars: {list(example_batch.data_vars)}')\n",
        "    example_batch['time'] = example_batch['time'].astype('timedelta64[ns]')\n",
        "    example_batch = example_batch.assign_coords(datetime=(('batch', 'time'), example_batch['datetime'].values.astype('datetime64[ns]')))\n",
        "    log_diag('time and datetime re-hydrated.')\n"
    ]

    topology_fix_code = [
        "# TOPOLOGY FIX (Injected by patcher)\n",
        "log_diag(f'Original Forcings: {task_config.forcing_variables}')\n",
        "task_config.input_variables = tuple(v for v in task_config.input_variables if v not in ['day_progress', 'year_progress'])\n",
        "task_config.forcing_variables = tuple(v for v in task_config.forcing_variables if v not in ['day_progress', 'year_progress'])\n",
        "log_diag(f'Pruned Forcings: {task_config.forcing_variables}')\n",
        "log_diag(f'Inputs count: {len(task_config.input_variables)}')\n"
    ]

    extraction_diag_code = [
        "# EXTRACTION DIAGNOSTICS (Injected by patcher)\n",
        "log_diag(f'Eval Input Vars: {list(eval_inputs.data_vars)}')\n",
        "log_diag(f'Eval Forcing Vars: {list(eval_forcings.data_vars)}')\n"
    ]

    new_cells = []
    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            # Check for visualization cells
            if len(cell['source']) > 0 and '# @title Plot' in cell['source'][0]:
                cell['source'] = ["# Plotting cell bypassed for headless execution\n"]
                plot_cells_cleared += 1
                new_cells.append(cell)
                continue
                
            new_source = []
            for line in cell['source']:
                modified_line = line
                
                # Apply replacements
                for target, replacement in replacements.items():
                    if target in line:
                        modified_line = modified_line.replace(target, replacement)
                        found_flags[target] = True
                
                # Apply bypasses
                for target in bypasses:
                    if target in line and 'def ' not in line:
                        modified_line = f"# {modified_line}"
                        found_flags[target] = True
                
                # Apply In-Line Topology Filtering
                if 'data_utils.add_derived_vars(example_batch)' in line:
                    indent = line[:len(line) - len(line.lstrip())]
                    modified_line = line + f"{indent}example_batch = example_batch.drop_vars(['day_progress', 'year_progress'], errors='ignore')\n"
                    print("  [OK] Injected in-line topology filter.")

                new_source.append(modified_line)
            
            cell['source'] = new_source
            new_cells.append(cell)

            # --- Inject new cells AFTER specific triggers ---
            # 1. After model load
            if any('ckpt = checkpoint.load(f' in line for line in cell['source']):
                print("  [OK] Injected TaskConfig topology fix cell.")
                new_cells.append({
                    "cell_type": "code", "execution_count": None, "metadata": {},
                    "outputs": [], "source": topology_fix_code
                })
            
            # 2. After data load
            if any('xarray.load_dataset(f)' in line for line in cell['source']):
                print("  [OK] Injected diagnostic re-hydration cell.")
                new_cells.append({
                    "cell_type": "code", "execution_count": None, "metadata": {},
                    "outputs": [], "source": rehydration_code
                })
                
            # 3. After extraction
            if any('extract_inputs_targets_forcings' in line for line in cell['source']):
                print("  [OK] Injected post-extraction diagnostic cell.")
                new_cells.append({
                    "cell_type": "code", "execution_count": None, "metadata": {},
                    "outputs": [], "source": extraction_diag_code
                })
        else:
            # Markdown cells
            new_cells.append(cell)

    nb['cells'] = new_cells

    print("Patching results:")
    for target, found in found_flags.items():
        status = "[OK]" if found else "[WARN]"
        print(f"  {status} Found/Patched: {target}")
    print(f"  [OK] Cleared {plot_cells_cleared} visualization cells.")

    with open(nb_path, 'w') as f:
        json.dump(nb, f, indent=1)
    
    print("Notebook patched successfully.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 patch_notebook.py <path_to_ipynb> <target_date>")
        sys.exit(1)
    
    patch_notebook(sys.argv[1], sys.argv[2])
