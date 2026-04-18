from kafka import KafkaProducer
import json
import os
from dotenv import load_dotenv

load_dotenv()

def get_producer():
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

def publish_post(producer, post, topic="posts.raw"):
    producer.send(
        topic,
        key=post.brand_id,
        value=post.model_dump(mode="json")
    )
    producer.flush()