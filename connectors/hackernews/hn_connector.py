import os
import time
import requests
import redis
import re
from datetime import datetime, timezone
from dotenv import load_dotenv
from models.post import Post
from models.publisher import get_producer, publish_post

load_dotenv()

HN_NEW_STORIES = "https://hn.algolia.com/api/v1/search_by_date?tags=story&hitsPerPage=200"
HN_ITEM = "https://hacker-news.firebaseio.com/v2/item/{}.json"
POLL_INTERVAL = int(os.getenv("HN_POLL_INTERVAL", 60))
SEEN_TTL = 86400  # 24 hours

TRACKED_KEYWORDS = [
    "openai", "nvidia", "apple", "google", "microsoft",
    "anthropic", "meta", "tesla", "amazon", "redis",
    "kafka", "python", "kubernetes"
]


def get_redis():
    return redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))


def already_seen(r, story_id):
    """Returns True if this story_id is in Redis."""
    return r.exists(f"hn:seen:{story_id}")


def mark_seen(r, story_id):
    """Store story_id in Redis with 24h expiry."""
    r.set(f"hn:seen:{story_id}", 1, ex=SEEN_TTL)

def fetch_new_story_ids():
    resp = requests.get(HN_NEW_STORIES, timeout=10)
    resp.raise_for_status()
    hits = resp.json()["hits"]
    return hits

def detect_brand(story):
    title = (story.get("title") or "").lower()
    for kw in TRACKED_KEYWORDS:
        if re.search(rf'\b{kw}\b', title):
            return kw
    return "general"

def is_relevant(story):
    if not story:
        return False
    title = (story.get("title") or "").lower()
    return any(re.search(rf'\b{kw}\b', title) for kw in TRACKED_KEYWORDS)

def story_to_post(story):
    return Post(
        id=f"hn:{story['objectID']}",
        text=story.get("title", ""),
        author_id=story.get("author", "unknown"),
        source="hackernews",
        brand_id=detect_brand(story),
        lang="en",
        timestamp=datetime.now(timezone.utc),
        metrics={
            "score": story.get("points") or 0,
            "comments": story.get("num_comments") or 0,
        }
    )

def run():
    print("Starting HN connector...")
    r = get_redis()
    producer = get_producer()

    while True:
        try:
            hits = fetch_new_story_ids()
            published = 0
            for story in hits:
                sid = story["objectID"]
                if already_seen(r, sid):
                    continue
                mark_seen(r, sid)
                if not is_relevant(story):
                    continue
                publish_post(producer, story_to_post(story))
                published += 1
            print(f"Published {published} relevant stories")
        except Exception as e:
            print(f"Poll error: {e}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()