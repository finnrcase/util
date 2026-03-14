import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.watttime_service import get_region

# UCSB coordinates
lat = 34.4139
lon = -119.8489

region = get_region(lat, lon)

print(region)