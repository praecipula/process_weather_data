
import json
import os
import re
from lxml import html
import random
from collections import defaultdict
from datetime import datetime

def parse_nws_row(row_html):
    cleaned = row_html.replace("&nbsp;", " ")
    root = html.fromstring("<div>" + cleaned + "</div>")
    tds = root.xpath(".//td")
    if not tds: return None
    
    time_str = tds[0].xpath("string()").strip()
    temp_val = tds[1].xpath("string()").strip()
    
    temp_f = None
    if temp_val and temp_val != "-" and temp_val != "":
        match = re.match(r"^-*[\d\.]+", temp_val)
        if match: temp_f = float(match.group())
            
    return time_str, temp_f

def analyze_patterns(station_dir, sample_rate=0.1):
    all_files = [os.path.join(station_dir, f) for f in os.listdir(station_dir) if not f.startswith('.')]
    sample_size = int(len(all_files) * sample_rate)
    sampled_files = random.sample(all_files, sample_size)
    
    print(f"Analyzing {len(sampled_files)} files (sampled from {len(all_files)})...")
    
    # Group stats by month (using first row of file as representative)
    monthly_stats = defaultdict(lambda: {"total": 0, "valid": 0})
    
    for i, fpath in enumerate(sampled_files):
        if i > 0 and i % 50 == 0: print(f"  Processed {i} files...")
        with open(fpath, 'r') as f:
            try:
                data = json.load(f)
                rows = data[0].get('rows', [])
                if not rows: continue
                
                # We need to infer the year. For KSFO files, they seem recent (2026).
                # But let's just use the "Month Day" as the key for now to see patterns.
                # Or try to find a full timestamp.
                
                for row_html in rows[1:]:
                    res = parse_nws_row(row_html)
                    if not res: continue
                    time_str, temp_f = res
                    
                    # time_str is like "Mar 17, 9:45 am"
                    match = re.match(r'^([A-Z][a-z]{2})\s+\d+', time_str)
                    if match:
                        month_key = match.group(1)
                    else:
                        month_key = "Unknown"
                        
                    monthly_stats[month_key]["total"] += 1
                    if temp_f is not None:
                        monthly_stats[month_key]["valid"] += 1
            except Exception:
                pass

    print("\n--- Temperature Data Density by Month Name ---")
    # Sort by calendar month?
    months_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Unknown"]
    for month in months_order:
        if month in monthly_stats:
            s = monthly_stats[month]
            ratio = s['valid'] / s['total'] if s['total'] > 0 else 0
            print(f"{month}: {s['valid']}/{s['total']} ({ratio:.1%})")

if __name__ == "__main__":
    analyze_patterns("input_scrapes/KSFO")
