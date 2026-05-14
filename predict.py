"""
predict.py
==========
Live forecasting and convergence analysis script.
Calculates uncertainty metrics and generates PNG/CSV outputs.
"""

import os
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import entropy

from lib.db import session
from lib.area_config import get_area
from lib.input_vector import SequenceBuilder
from lib.keras_model import WeatherLSTM

def calculate_uncertainty(probs):
    ent = entropy(probs)
    sorted_probs = np.sort(probs)[::-1]
    n_buckets_90 = np.where(np.cumsum(sorted_probs) >= 0.90)[0][0] + 1
    return ent, n_buckets_90

def run_prediction_progression(area_key: str, station_code: str, target_date: datetime.date):
    print(f"--- Convergence Analysis for {station_code} on {target_date} ---")
    
    area = get_area(area_key)
    builder = SequenceBuilder(area, session)
    model_path = os.path.join("models", f"best_weather_{area_key}.keras")
    
    weather_model = WeatherLSTM(area)
    weather_model.model.load_weights(model_path)
    
    # Build Context Head (Yesterday back to 7 days ago)
    # This is constant for the whole progression
    X_context = builder.build_context_sequence(station_code, target_date)
    X_context = np.expand_dims(X_context, axis=0) # Batch dimension
    
    results = []
    tz = builder.tz
    for hour in range(24):
        local_as_of = datetime.datetime.combine(target_date, datetime.time(hour, 0)).replace(tzinfo=tz)
        utc_as_of = local_as_of.astimezone(datetime.timezone.utc)
        
        # Build Live Head as of this hour
        X_live = builder.build_partial_day_sequence(station_code, target_date, as_of_utc=utc_as_of)
        X_live = np.expand_dims(X_live, axis=0)
        
        # Predict using both inputs
        max_probs, _ = weather_model.predict_probs({"main_input": X_live, "context_input": X_context})
        
        max_probs = max_probs[0]
        top_anomaly_idx = np.argmax(max_probs)
        top_anomaly = top_anomaly_idx + area.anomaly_bucket_min_f
        
        ent, ci90 = calculate_uncertainty(max_probs)
        
        summary = builder._get_summary(station_code, target_date)
        actual_max = summary.max_temp_f if summary else None
        normal_max = (summary.max_temp_normal if summary else None) or 65.0
        predicted_temp = top_anomaly + normal_max
        
        results.append({
            "hour_local": hour,
            "predicted_temp": predicted_temp,
            "actual_temp": actual_max,
            "ci90_width": ci90,
            "probs": max_probs
        })
        print(f"  {hour:02d}:00 -> Pred: {predicted_temp:.1f}F (Conf: {max_probs[top_anomaly_idx]:.1%}, 90%CI: {ci90} deg)")

    df = pd.DataFrame(results)
    csv_path = f"prediction_{station_code}_{target_date}.csv"
    df.drop(columns=['probs']).to_csv(csv_path, index=False)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10))
    ax1.plot(df["hour_local"], df["predicted_temp"], 'b-o', label="Predicted High")
    if actual_max is not None: ax1.axhline(y=actual_max, color='r', linestyle='--', label=f"Actual High ({actual_max}F)")
    ax1.set_ylabel("Temperature (F)")
    ax1.set_title(f"Forecast Convergence: {station_code} ({target_date})")
    ax1.legend(); ax1.grid(True)
    ax2.plot(df["hour_local"], df["ci90_width"], 'g-s', label="90% Confidence Interval Width")
    ax2.set_ylabel("Uncertainty (Deg)"); ax2.set_ylim(0, 15); ax2.legend(); ax2.grid(True)
    plt.savefig(f"convergence_{station_code}_{target_date}.png")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--station", default="KSFO")
    parser.add_argument("--date", default="2024-04-15")
    args = parser.parse_args()
    run_prediction_progression("sfbay", args.station, datetime.date.fromisoformat(args.date))
