from kafka import KafkaProducer
from dotenv import load_dotenv
import praw
import json
import time
import os

load_dotenv()

def reddit_stream(subreddit_name, client_id, client_secret, user_agent):
    """
    Streams new posts from a specified subreddit.

    Args:
        subreddit_name (str): Name of the subreddit to stream posts from.
        client_id (str): Your Reddit app client ID.
        client_secret (str): Your Reddit app client secret.
        user_agent (str): A user agent string describing your app.

    Yields:
        dict: Information about a new Reddit post.
    """
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )
    subreddit = reddit.subreddit(subreddit_name)
    print(f"Listening for new posts in r/{subreddit_name}...")
    for submission in subreddit.stream.submissions(skip_existing=False):
        yield {
            "title": submission.title,
            "author": str(submission.author),
            "created_utc": submission.created_utc,
            "url": submission.url,
            "score": submission.score,
            "id": submission.id,
            "subreddit": str(submission.subreddit)
        }

def kafka_producer(broker):
    """
    Creates a Kafka producer instance.

    Args:
        broker (str): Kafka broker address.

    Returns:
        KafkaProducer: A Kafka producer instance.
    """
    return KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def publish_to_kafka(producer, topic, message):
    """
    Publishes a message to a Kafka topic.

    Args:
        producer (KafkaProducer): Kafka producer instance.
        topic (str): Topic name.
        message (dict): Message to send.
    """
    producer.send(topic, value=message)
    producer.flush()

if __name__ == "__main__":
    CLIENT_ID = os.getenv('CLIENT_ID')
    CLIENT_SECRET = os.getenv('CLIENT_SECRET')
    USER_AGENT = os.getenv('USER_AGENT')
    
    subreddits = ['health', 'Music', 'movies', 'sports', 'books', 'space', 'science', 'Art', 'history', 'politics', 'technology']

    KAFKA_BROKER = "localhost:9092"
    KAFKA_TOPIC = "reddit_stream"

    producer = kafka_producer(KAFKA_BROKER)
    
    MAX_POSTS = 90

    try:
        for subreddit in subreddits:
            post_count = 0
            for post in reddit_stream(subreddit, CLIENT_ID, CLIENT_SECRET, USER_AGENT):
                if post_count >= MAX_POSTS:
                    print(f"Reached the limit of {MAX_POSTS} posts for subreddit {subreddit}. Stopping...")
                    break
                
                print(f"Publishing to Kafka: {post}")
                publish_to_kafka(producer, KAFKA_TOPIC, post)
                post_count += 1
                time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStreaming stopped.")
    except Exception as e:
        print(f"Error: {e}")