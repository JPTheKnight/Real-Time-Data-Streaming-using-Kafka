from kafka import KafkaConsumer
from transformers import pipeline
import json
import csv
import re

# Initialize the sentiment analysis pipeline from Hugging Face
sentiment_analyzer = pipeline("sentiment-analysis")

def clean_text(text):
    """
    Cleans the input text by removing unwanted characters, links, and extra spaces.

    Args:
        text (str): The text to clean.

    Returns:
        str: Cleaned text.
    """
    # Remove URLs
    text = re.sub(r'http\S+|www\.\S+', '', text)
    # Remove special characters and numbers
    text = re.sub(r'[^A-Za-z\s]', '', text)
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def analyze_sentiment(text):
    """
    Analyzes the sentiment of a given text using a pre-trained model.

    Args:
        text (str): The text to analyze.

    Returns:
        str: Sentiment label (e.g., POSITIVE, NEGATIVE).
        float: Confidence score (0 to 1).
    """
    result = sentiment_analyzer(text)[0]
    return result['label'], result['score']

if __name__ == "__main__":
    KAFKA_BROKER = "localhost:9092"  # Change to your Kafka broker address
    KAFKA_TOPIC = "twitter_stream"
    OUTPUT_CSV = "twitter_sentiment_analysis.csv"

    # Initialize Kafka Consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    # Open CSV file for writing
    with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Title", "Sentiment", "Confidence"])

        print(f"Listening to Kafka topic '{KAFKA_TOPIC}' for sentiment analysis...")

        try:
            for message in consumer:
                post = message.value
                text = post.get("text", "")

                if text:
                    cleaned_text = clean_text(text)
                    sentiment, confidence = analyze_sentiment(cleaned_text)
                    print(f"Post: {text}")
                    print(f"Sentiment: {sentiment} (Confidence: {confidence})\n")

                    # Write to CSV
                    writer.writerow([cleaned_text, sentiment, confidence])

        except KeyboardInterrupt:
            print("\nStopped listening to Kafka stream.")
        except Exception as e:
            print(f"Error: {e}")
