from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class Post(BaseModel):
    id: str
    text: str
    author_id: str
    source: str          # 'twitter' | 'reddit' | 'news'
    brand_id: str
    lang: Optional[str] = None
    timestamp: datetime
    metrics: dict = {}

    # filled in later by NLP workers
    sentiment: Optional[str] = None
    sentiment_score: Optional[float] = None
    entities: list = []
    topics: list = []