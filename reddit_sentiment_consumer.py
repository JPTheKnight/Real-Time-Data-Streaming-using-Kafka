from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline
import nltk
import json
import re

try:
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt_tab')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

sentiment_analyzer = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment")

def clean_text(text):
    text = re.sub(r'http\S+|www\.\S+', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def analyze_sentiment(text):
    """
    Analyzes the sentiment of a given text using a pre-trained model.

    Args:
        text (str): The text to analyze.

    Returns:
        str: Sentiment label (e.g., POSITIVE, NEGATIVE, NEUTRAL).
        float: Confidence score (0 to 1).
    """
    result = sentiment_analyzer(text)[0]
    return result['label'], result['score']

if __name__ == "__main__":
    KAFKA_BROKER = "localhost:9092"
    INPUT_TOPIC = "reddit_stream"
    OUTPUT_TOPIC = "sentiment_analyzed"

    label_mapping = {
        "LABEL_0": "Negative",
        "LABEL_1": "Neutral",
        "LABEL_2": "Positive"
    }

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id='reddit-sentiment-group'
    )

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"Listening to Kafka topic '{INPUT_TOPIC}' for sentiment analysis...")

    try:
        for message in consumer:
            post = message.value
            partition = message.partition
            title = post.get("title", "")

            if title:
                cleaned_title = clean_text(title)
                sentiment, confidence = analyze_sentiment(cleaned_title)
                sentiment = label_mapping[sentiment]

                output_message = {
                    "title": cleaned_title,
                    "sentiment": sentiment,
                    "confidence": confidence
                }

                print(f"Post: {cleaned_title} (Partition: {partition})")
                print(f"Sentiment: {sentiment} (Confidence: {confidence:.2f})\n")

                producer.send(OUTPUT_TOPIC, output_message)

    except KeyboardInterrupt:
        print("\nStopped listening to Kafka stream.")
    except Exception as e:
        print(f"Error: {e}")
