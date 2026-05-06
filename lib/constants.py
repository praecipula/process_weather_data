"""
constants.py
============
Shared dimensions and constants for the weather prediction pipeline.
"""

MAX_SEQ_LEN = 288        # 24h * 12 five-minute slots
N_FEATURES = 80          # Updated size for grammar-aware cloud encoding
