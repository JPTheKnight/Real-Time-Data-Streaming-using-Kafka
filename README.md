# Real-Time Reddit Data Streaming and NLP Analysis

This project demonstrates a real-time data streaming pipeline using Apache Kafka to process Reddit posts dynamically. It integrates advanced natural language processing (NLP) techniques such as sentiment analysis and topic modeling using Hugging Face Transformers and BERTopic.

---

## Features

1. **Producer**:
    - Fetches posts from Reddit using the PRAW API.
    - Streams the posts into Kafka topics.
    - Supports partitioning for efficient load balancing.

2. **Consumers**:
    - **Sentiment Analysis Consumer**:
        - Analyzes the sentiment of Reddit posts (positive, negative, or neutral).
    - **Topic Extraction Consumer**:
        - Performs topic modeling on post titles using BERTopic.
        - Categorizes posts and saves structured results to a CSV file.

3. **Model Training**:
    - Trains a BERTopic model using Reddit titles.
    - Saves the model for efficient reuse.

4. **Visualization**:
    - Generates word clouds to visualize common topics.

---

## Architecture Overview

- **Two Kafka topics**:
    - `reddit_stream`: Primary topic for streaming Reddit posts.
    - `sentiment_analyzed`: Secondary topic for sentiment-enriched data.
- **Producer**:
    - Streams Reddit posts to `reddit_stream`.
- **Consumers**:
    - Sentiment analysis consumers (in `reddit-sentiment-group`) analyze data and produce results to `sentiment_analyzed`.
    - Topic extraction consumers (in `reddit-topic-group`) read from `sentiment_analyzed` for topic modeling.

---

## Prerequisites

- Python 3.7+
- Apache Kafka
- Required Python packages:
  ```bash
  pip install kafka-python praw nltk bertopic umap-learn matplotlib wordcloud transformers
  ```
- A valid Reddit API client (register at https://www.reddit.com/prefs/apps).

---

## Setup

### 1. Configure Kafka
1. Start Zookeeper and Kafka brokers (yourself or you can try with run_kafka.bat if you're on windows).
2. Create required topics:
   ```bash
   bin/kafka-topics.sh --create --topic reddit_stream --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
   bin/kafka-topics.sh --create --topic sentiment_analyzed --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
   ```

### 2. Configure Reddit API
1. Add your Reddit API credentials to the producer script:
   ```python
   CLIENT_ID = "your_client_id"
   CLIENT_SECRET = "your_client_secret"
   USER_AGENT = "your_user_agent"
   ```

### 3. Run the Producer
1. Start the producer to stream posts:
   ```bash
   python reddit_streamer.py
   ```

### 4. Run Consumers
1. Start sentiment analysis consumers:
   ```bash
   python reddit_sentiment_consumer.py
   ```
2. Start topic extraction consumers:
   ```bash
   python reddit_topic_consumer.py
   ```

### 5. Train BERTopic Model (Optional)
To retrain the BERTopic model:
```bash
python training_bert.py
```

---

## Example Outputs

### Sentiment Analysis Consumer
```plaintext
Post: ISRO successfully docks two satellites in space, India fourth country to achieve feat after US, Russia, China (Partition: 0)
Sentiment: Positive (Confidence: 0.74)

Post: Poppy Playtime Sues Google for Failing to Remove Copyright Infringing 'Scam' Apps (Partition: 0)
Sentiment: Negative (Confidence: 0.81)
```

### Topic Extraction Consumer
```plaintext
Are You Experienced by Jimi Hendrix, Neutral, 0.8792815208435059, "rock, actions, around, one, korn, sparks, woodstock, dokken, gilded"

is there a missing godfather 2 love scene?, Neutral, 0.8356384038925171, "movie, movies, best, poster, films, starring, film, comedy, official, trailer"
```

### Word Cloud
A word cloud is generated at the end of topic extraction to visualize common topics.

---

## Project Structure
```plaintext
.
├── reddit_producer.py         # Streams Reddit posts to Kafka
├── sentiment_consumer.py      # Performs sentiment analysis on posts
├── topic_consumer.py          # Extracts topics from processed data
├── train_bertopic.py          # Trains and saves the BERTopic model
├── bertopic_model             # Saved BERTopic model
├── wordcloud.png              # Generated word cloud
├── requirements.txt           # Python dependencies
└── README.md                  # Project documentation
```

## License
This project is licensed under the MIT License.
