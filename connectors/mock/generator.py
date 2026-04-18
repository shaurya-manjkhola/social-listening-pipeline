import os, time, random, argparse
from datetime import datetime, timezone
from dotenv import load_dotenv
from models.post import Post
from models.publisher import get_producer, publish_post

load_dotenv()

BRANDS = ["openai", "anthropic", "nvidia", "apple", "google", "microsoft", "amazon", "meta"]

TEMPLATES = [
    "{brand} just made a major announcement",
    "Everyone is talking about {brand} today",
    "Is {brand} in trouble? Experts weigh in",
    "{brand} stock surges after news",
    "Breaking: {brand} releases something unexpected",
    "{brand} under fire from users",
    "Why {brand} is dominating the news cycle",
    "{brand} partnership could change everything",
]

def make_post(brand_id):
    return Post(
        id=f"mock:{brand_id}:{time.time_ns()}",
        text=random.choice(TEMPLATES).format(brand=brand_id),
        author_id=f"mock_user_{random.randint(1, 1000)}",
        source="mock",
        brand_id=brand_id,
        lang="en",
        timestamp=datetime.now(timezone.utc),
        metrics={}
    )

def run(posts_per_second=10, duration=None, brand=None):
    producer = get_producer()
    interval = 1.0 / posts_per_second
    count = 0
    print(f"Generating {posts_per_second} posts/sec" + (f" for {duration}s" if duration else " (Ctrl+C to stop)"))
    while True:
        b = brand or random.choice(BRANDS)
        publish_post(producer, make_post(b))
        count += 1
        if duration and count >= posts_per_second * duration:
            print(f"Done. Published {count} posts.")
            break
        time.sleep(interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=10, help="Posts per second")
    parser.add_argument("--duration", type=int, default=None, help="Seconds to run")
    parser.add_argument("--brand", type=str, default=None, help="Target brand")
    parser.add_argument("--spike", action="store_true", help="10x rate spike for 120s")
    args = parser.parse_args()

    if args.spike:
        run(posts_per_second=100, duration=120, brand=args.brand)
    else:
        run(posts_per_second=args.rate, duration=args.duration, brand=args.brand)