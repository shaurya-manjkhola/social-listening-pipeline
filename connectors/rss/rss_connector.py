import os, time, re, redis, feedparser
from datetime import datetime, timezone
from dotenv import load_dotenv
from models.post import Post
from models.publisher import get_producer, publish_post

load_dotenv()

POLL_INTERVAL = 60
SEEN_TTL = 86400

FEEDS = [
    "https://feeds.feedburner.com/TechCrunch",
    "https://www.theverge.com/rss/index.xml",
    "https://feeds.arstechnica.com/arstechnica/index",
    "https://www.wired.com/feed/rss",
]

TRACKED = {
    "openai": ["openai", "chatgpt"],
    "anthropic": ["anthropic", "claude"],
    "nvidia": ["nvidia"],
    "apple": ["apple"],
    "google": ["google", "gemini"],
    "microsoft": ["microsoft", "copilot"],
    "python": ["python"],
    "kubernetes": ["kubernetes", "k8s"],
    "amazon": ["amazon", "aws"],
    "meta": ["meta", "llama"],
}


def get_redis():
    return redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))


def detect_brand(text):
    text = text.lower()
    for brand, keywords in TRACKED.items():
        if any(re.search(rf'\b{kw}\b', text) for kw in keywords):
            return brand
    return None


def run():
    print("Starting RSS connector...")
    r = get_redis()
    producer = get_producer()

    while True:
        published = 0
        for feed_url in FEEDS:
            try:
                feed = feedparser.parse(feed_url)
                for entry in feed.entries:
                    eid = entry.get("id") or entry.get("link")
                    if not eid:
                        continue
                    key = f"rss:seen:{eid}"
                    if r.exists(key):
                        continue
                    r.set(key, 1, ex=SEEN_TTL)
                    title = entry.get("title", "")
                    brand = detect_brand(title)
                    if not brand:
                        continue
                    published_time = entry.get("published_parsed")
                    timestamp = datetime(*published_time[:6], tzinfo=timezone.utc) if published_time else datetime.now(timezone.utc)
                    post = Post(
                        id=f"rss:{eid}",
                        text=title,
                        author_id=entry.get("author", "unknown"),
                        source="rss",
                        brand_id=brand,
                        lang="en",
                        timestamp=timestamp,
                        metrics={}
                    )
                    publish_post(producer, post)
                    published += 1
            except Exception as e:
                print(f"Feed error ({feed_url}): {e}")

        print(f"Published {published} RSS articles")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()