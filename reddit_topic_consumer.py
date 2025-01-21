from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from kafka import KafkaConsumer
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from bertopic import BERTopic
from PIL import Image
import numpy as np
import datetime
import nltk
import json
import csv
import re

try:
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt_tab')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

topic_counter = Counter()

def clean_and_tokenize(text):
    text = re.sub(r'http\S+|www\.\S+', '', text)
    text = re.sub(r'[^A-Za-z\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    tokens = word_tokenize(text.lower())
    stop_words = set(stopwords.words('english'))
    return [word for word in tokens if word not in stop_words]

try:
    topic_model = BERTopic().load("bertopic_model")
    print("Loaded pre-trained BERTopic model.")
except Exception as e:
    print(f"Error loading BERTopic model: {e}")
    exit()

if __name__ == "__main__":
    KAFKA_BROKER = "localhost:9092"
    KAFKA_TOPIC = "sentiment_analyzed"
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    OUTPUT_CSV = f"reddit_post_categories_{timestamp}.csv"
    
    try:
        reddit_mask = Image.open('image.png').convert("L")
        threshold = 128
        reddit_mask = np.array(reddit_mask.point(lambda p: p > threshold and 255))
    except FileNotFoundError:
        print(f"Mask image image.png not found. Using default circular mask.")
        reddit_mask = None

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id='reddit-topic-group'
    )

    with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Title", "Sentiment", "Confidence", "Topic Description"])

        print(f"Listening to Kafka topic '{KAFKA_TOPIC}' for topic extraction and categorization...")

        try:
            for message in consumer:
                post = message.value
                partition = message.partition
                title = post.get("title", "")

                if title:
                    cleaned_title = ' '.join(clean_and_tokenize(title))
                    predicted_topic, _ = topic_model.transform(cleaned_title)
                    topic_id = predicted_topic[0]

                    if topic_id == -1:
                        topic_description = "Outlier / Uncategorized"
                    else:
                        topic_words = topic_model.get_topic(topic_id)
                        topic_description = ", ".join([word for word, _ in topic_words])

                    print(f"Post: {cleaned_title} (Partition: {partition})")
                    print(f"Predicted Topic ID: {topic_id}")
                    print(f"Topic Description: {topic_description}\n")
                    
                    topic_counter.update(clean_and_tokenize(title))

                    writer.writerow([title, post.get("sentiment", ""), post.get("confidence", ""), topic_description])

        except KeyboardInterrupt:
            print("\nStopped listening to Kafka stream.")

    wordcloud = WordCloud(width=800, height=500, random_state=42, max_font_size=100, mask=reddit_mask,
            contour_width=0, contour_color='black', background_color="white").generate_from_frequencies(topic_counter)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.title("Most Common Topics")
    plt.show()

    print("Analysis complete. CSV file and word cloud generated.")
