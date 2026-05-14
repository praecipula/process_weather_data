"""
constants.py
============
Shared dimensions and constants for the Multi-Resolution pipeline.
"""

MACRO_SEQ_LEN = 24       # 24 hours of 1-hour slots
MICRO_SEQ_LEN = 24       # 120 minutes of 5-minute slots
N_FEATURES = 15          # Optimized Quintet: [P, I, D, Time, Hum, Wind, Weather, Sky]
