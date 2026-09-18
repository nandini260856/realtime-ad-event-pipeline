"""Generate synthetic ad impression events."""

import json
import random
import time
import uuid
from datetime import datetime, timezone

CAMPAIGNS = ["cc_zero_summer", "sprite_gaming", "smartwater_fitness", "fanta_backtoschool"]
PLACEMENTS = ["app_store_search", "news_feed", "maps_local", "video_preroll"]

def make_event():
    """Build one ad impression event."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "campaign_id": random.choice(CAMPAIGNS),
        "placement": random.choice(PLACEMENTS),
        "user_hash": f"u{random.randint(1, 5000):05d}",
        "event_type": random.choices(["impression", "click"], weights=[95, 5])[0],
        "bid_cpm": round(random.uniform(0.80, 12.50), 2),
    }

if __name__ == "__main__":
    for _ in range(10):
        print(json.dumps(make_event()))
        time.sleep(0.2)