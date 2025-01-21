from kafka import KafkaProducer
import tweepy
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def twitter_stream(query, bearer_token, size=10, limit=20):
    """
    Streams tweets based on a specified query.

    Args:
        query (str): The search query for tweets.
        bearer_token (str): Bearer token for Twitter API authentication.
        size (int): Maximum number of results per API request.
        limit (int): Total number of tweets to fetch.

    Yields:
        dict: Information about a tweet.
    """
    client = tweepy.Client(bearer_token=bearer_token)
    logging.info(f"Starting to fetch tweets for query: {query}")
    try:
        for tweet in tweepy.Paginator(
            client.search_recent_tweets,
            query=query,
            tweet_fields=['created_at', 'lang', 'author_id'],
            max_results=size
        ).flatten(limit=limit):
            yield {
                "id": tweet.id,
                "text": tweet.text,
                "created_at": tweet.created_at.isoformat(),
                "lang": tweet.lang,
                "author_id": tweet.author_id,
            }
    except tweepy.errors.TooManyRequests as e:
        reset_time = int(e.response.headers.get('x-rate-limit-reset', time.time() + 15 * 60))
        sleep_time = reset_time - int(time.time())
        logging.warning(f"Rate limit exceeded. Sleeping for {sleep_time} seconds...")
        time.sleep(max(sleep_time, 0) + 1)
        yield from twitter_stream(query, bearer_token, size, limit)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

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
    producer.send(topic, message)
    producer.flush()

if __name__ == "__main__":
    # Replace these with your Twitter API credentials and Kafka settings
    BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAOijyAEAAAAAuBcaNdEEXZuZT3y1ehd%2FNI4DdIQ%3DH1FvdnhJWkIt0QMGdbeUNfODQWM0mphajAbNT3LVhidBWzX0IL'
    QUERY = 'France'  # Change this to your desired search query
    KAFKA_BROKER = 'localhost:9092'  # Change to your Kafka broker address
    KAFKA_TOPIC = 'twitter_stream'

    producer = kafka_producer(KAFKA_BROKER)

    try:
        for tweet in twitter_stream(QUERY, BEARER_TOKEN):
            logging.info(f"Publishing to Kafka: {tweet}")
            publish_to_kafka(producer, KAFKA_TOPIC, tweet)
            time.sleep(1)  # Optional delay to avoid overwhelming Kafka
    except KeyboardInterrupt:
        logging.info("\nStreaming stopped.")
    except Exception as e:
        logging.error(f"Error: {e}")
