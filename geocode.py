import pandas as pd
import os
from geopy.geocoders import Photon
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

save_path = 'data/geocoded_2021_2026.csv'

# Initialise geocoder
geolocator = Photon(user_agent="dublin_trees_ppr")
geocode = RateLimiter(
    geolocator.geocode,
    min_delay_seconds=0.2,
    error_wait_seconds=5,
    max_retries=3,
    swallow_exceptions=True
)

# Load existing progress from file
if os.path.exists(save_path):
    ppr_to_geocode = pd.read_csv(save_path, low_memory=False)
    already_done = ppr_to_geocode['latitude'].notna().sum()
    remaining = ppr_to_geocode['latitude'].isna().sum()
    print(f"Resuming from saved file")
    print(f"Already geocoded: {already_done}")
    print(f"Remaining: {remaining}")
    print(f"Progress: {already_done / len(ppr_to_geocode) * 100:.1f}%")
else:
    print("Error")

# Run geocoding with checkpoints
errors = 0
for count, (i, row) in enumerate(tqdm(ppr_to_geocode.iterrows(), total=len(ppr_to_geocode))):
    if pd.notna(ppr_to_geocode.at[i, 'latitude']):
        continue

    try:
        location = geocode(str(row['Address']) + ', Dublin, Ireland')
        if location:
            ppr_to_geocode.at[i, 'latitude'] = location.latitude
            ppr_to_geocode.at[i, 'longitude'] = location.longitude
            ppr_to_geocode.at[i, 'geocode_status'] = 'ok'
        else:
            ppr_to_geocode.at[i, 'geocode_status'] = 'not_found'
            errors += 1
    except Exception as e:
        ppr_to_geocode.at[i, 'geocode_status'] = f'error: {str(e)}'
        errors += 1

    if count % 1000 == 0:
        ppr_to_geocode.to_csv(save_path, index=False)
        success = ppr_to_geocode['latitude'].notna().sum()
        print(f"\nCheckpoint saved | Geocoded: {success} | Errors: {errors}")

# Final save
ppr_to_geocode.to_csv(save_path, index=False)

# Summary
total = len(ppr_to_geocode)
success = ppr_to_geocode['latitude'].notna().sum()
not_found = (ppr_to_geocode['geocode_status'] == 'not_found').sum()
print(f"\nDone")
print(f"Total: {total}")
print(f"Successfully geocoded: {success} ({success/total*100:.1f}%)")
print(f"Not found: {not_found}")
print(f"Errors: {errors}")