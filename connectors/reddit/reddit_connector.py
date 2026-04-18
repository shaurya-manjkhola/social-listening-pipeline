import os, time, requests, redis, re
from datetime import datetime, timezone
from dotenv import load_dotenv
from models.post import Post
from models.publisher import get_producer, publish_post

load_dotenv()

BASE = "https://arctic-shift.photon-reddit.com/api/posts/search"
POLL_INTERVAL = 60
SEEN_TTL = 86400

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

SUBREDDITS = ["technology", "programming", "MachineLearning", "artificial", "singularity"]


def get_redis():
    return redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))


def already_seen(r, pid):
    return r.exists(f"reddit:seen:{pid}")


def mark_seen(r, pid):
    r.set(f"reddit:seen:{pid}", 1, ex=SEEN_TTL)


def detect_brand(text):
    text = text.lower()
    for brand, keywords in TRACKED.items():
        if any(re.search(rf'\b{kw}\b', text) for kw in keywords):
            return brand
    return None




def fetch_posts():
    posts = []
    for sub in SUBREDDITS:
        try:
            resp = requests.get(BASE, params={
                "subreddit": sub,
                "limit": 20,
            }, timeout=10)
            resp.raise_for_status()
            posts.extend(resp.json().get("data") or [])
        except Exception as e:
            print(f"Fetch error ({sub}): {e}")
    return posts

def run():
    print("Starting Reddit connector...")
    r = get_redis()
    producer = get_producer()

    while True:
        posts = fetch_posts()
        published = 0
        for p in posts:
            pid = p.get("id")
            if not pid or already_seen(r, pid):
                continue
            mark_seen(r, pid)
            title = p.get("title", "")
            brand = detect_brand(title)
            if not brand:
                continue
            post = Post(
                id=f"reddit:{pid}",
                text=title,
                author_id=p.get("author", "unknown"),
                source="reddit",
                brand_id=brand,
                lang="en",
                timestamp=datetime.fromtimestamp(
                    p.get("created_utc", 0), tz=timezone.utc
                ),
                metrics={
                    "score": p.get("score", 0),
                    "comments": p.get("num_comments", 0),
                }
            )
            publish_post(producer, post)
            published += 1
        print(f"Published {published} Reddit posts")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()