# connectors/mock/generator.py — skeleton
import time, random
from datetime import datetime, timezone
from models.post import Post
from models.publisher import get_producer, publish_post

BRANDS = ["nike", "apple", "openai", "nvidia"]
TEMPLATES = [
    "{brand} just announced something huge",
    "Can't believe what {brand} did today",
    "Everyone is talking about {brand}",
    "Is {brand} going to recover from this?",
    "{brand} stock is moving",
]

def make_post(brand_id):
    return Post(
        id=f"mock:{brand_id}:{time.time_ns()}",
        text=random.choice(TEMPLATES).format(brand=brand_id),
        author_id=f"mock_user_{random.randint(1,1000)}",
        source="mock",
        brand_id=brand_id,
        lang="en",
        timestamp=datetime.now(timezone.utc),
        metrics={}
    )

def run(posts_per_second=10, duration_seconds=None):
    producer = get_producer()
    interval = 1.0 / posts_per_second
    count = 0
    while True:
        brand = random.choice(BRANDS)
        publish_post(producer, make_post(brand))
        count += 1
        if duration_seconds and count >= posts_per_second * duration_seconds:
            break
        time.sleep(interval)