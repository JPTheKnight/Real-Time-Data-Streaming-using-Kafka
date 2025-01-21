from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from kafka import KafkaConsumer
from bertopic import BERTopic
from umap import UMAP
import json
import nltk
import re

try:
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt_tab')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

def clean_and_tokenize(text):
    text = re.sub(r'http\S+|www\.\S+', '', text)
    text = re.sub(r'[^A-Za-z\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    tokens = word_tokenize(text.lower())
    stop_words = set(stopwords.words('english'))
    return ' '.join([word for word in tokens if word not in stop_words])

if __name__ == "__main__":
    KAFKA_BROKER = "localhost:9092"
    KAFKA_TOPIC = "reddit_stream"

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    titles = []

    print(f"Listening to Kafka topic '{KAFKA_TOPIC}' for collecting titles...")

    try:
        for message in consumer:
            post = message.value
            title = post.get("title", "")

            if title:
                cleaned_title = clean_and_tokenize(title)
                titles.append(cleaned_title)

                print(f"Collected Title: {cleaned_title}")

    except KeyboardInterrupt:
        print("\nStopped listening to Kafka stream.")
        print(f"Collected {len(titles)} titles.")

umap_model = UMAP(n_neighbors=15, 
                  n_components=5, 
                  min_dist=0.0, 
                  metric='cosine', 
                  random_state=100)

topic_model = BERTopic(umap_model=umap_model, language="english", calculate_probabilities=True)
topics, probs = topic_model.fit_transform(titles)

topic_model.save("bertopic_model")
