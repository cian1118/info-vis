import pandas as pd
import os
import shutil
from geopy.geocoders import Photon
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

save_path = 'data/new_geocoded_2021_2026.csv'

def safe_save(df, path):
    temp_path = path + '.tmp'
    df.to_csv(temp_path, index=False)
    shutil.move(temp_path, path)

geolocator = Photon(user_agent="dublin_trees_ppr")
geocode = RateLimiter(
    geolocator.geocode,
    min_delay_seconds=0.2,
    error_wait_seconds=5,
    max_retries=3,
    swallow_exceptions=True
)

df = pd.read_csv(save_path, low_memory=False).reset_index(drop=True)
tqdm.write(f"Loaded {len(df)} rows | Done: {df['latitude'].notna().sum()} | Remaining: {df['latitude'].isna().sum()}")

# Find first row without geocode_status by position
start_pos = df['geocode_status'].isna().idxmax()
tqdm.write(f"Starting from position {start_pos}")

errors = 0
for count, i in enumerate(tqdm(range(start_pos, len(df)), total=len(df) - start_pos)):
    if pd.notna(df.at[i, 'geocode_status']):
        continue

    try:
        location = geocode(str(df.at[i, 'Address']) + ', Dublin, Ireland')
        if location:
            df.at[i, 'latitude'] = location.latitude
            df.at[i, 'longitude'] = location.longitude
            df.at[i, 'geocode_status'] = 'ok'
        else:
            df.at[i, 'geocode_status'] = 'not_found'
            errors += 1
    except Exception as e:
        df.at[i, 'geocode_status'] = f'error: {str(e)}'
        errors += 1

    if count % 1000 == 0:
        safe_save(df, save_path)
        tqdm.write(f"Checkpoint | Done: {df['latitude'].notna().sum()} | Errors: {errors}")

safe_save(df, save_path)

total = len(df)
success = df['latitude'].notna().sum()
not_found = (df['geocode_status'] == 'not_found').sum()
tqdm.write(f"Done | Total: {total} | Success: {success} ({success/total*100:.1f}%) | Not found: {not_found} | Errors: {errors}")