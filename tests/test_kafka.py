from models.post import Post
from models.publisher import get_producer, publish_post
from datetime import datetime, timezone

post = Post(
    id="test:001",
    text="Nike just dropped new shoes and everyone is talking about it",
    author_id="user_123",
    source="twitter",
    brand_id="nike",
    lang="en",
    timestamp=datetime.now(timezone.utc),
    metrics={"likes": 42}
)

producer = get_producer()
publish_post(producer, post)
print("Published post to Kafka successfully")