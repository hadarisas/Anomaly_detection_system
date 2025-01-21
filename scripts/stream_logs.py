import time
import json
import logging
import random
import subprocess
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
from contextlib import closing

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stream_logs.log"),
        logging.StreamHandler()
    ]
)

class LogStreamer:
    def __init__(self):
        # Load configuration from environment variables
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'hadoop-logs')
        self.hdfs_path = os.getenv('HDFS_PATH', '/logs/hadoop-logs.log')

        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logging.info("Kafka producer initialized.")
        except Exception as e:
            logging.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def read_hdfs_file(self, hdfs_path):
        """Stream file from HDFS using hdfs dfs -cat command via subprocess."""
        cmd = f"docker-compose exec namenode hdfs dfs -cat {hdfs_path}"
        try:
            with subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, text=True) as proc:
                for line in proc.stdout:
                    yield line.strip()
        except Exception as e:
            logging.error(f"Error reading HDFS file: {e}")
            return

    def stream_logs(self, min_delay=1, max_delay=3):
        """Stream logs from HDFS file with random delay between messages."""
        logging.info(f"Streaming logs from HDFS path: {self.hdfs_path}")
        try:
            for line in self.read_hdfs_file(self.hdfs_path):
                if not line:
                    continue  # Skip empty lines
                
                # Remove line numbers if present (e.g., "123|")
                if '|' in line:
                    line = line.split('|', 1)[1].strip()

                # Send log to Kafka
                self.producer.send(self.kafka_topic, {'log': line})
                self.producer.flush()
                logging.info(f"Sent to Kafka topic '{self.kafka_topic}': {line}")

                # Random delay between messages
                delay = random.uniform(min_delay, max_delay)
                time.sleep(delay)
        except KeyboardInterrupt:
            logging.info("Log streaming interrupted by user.")
        except Exception as e:
            logging.error(f"Unexpected error during log streaming: {e}")
        finally:
            self.close_producer()

    def close_producer(self):
        """Close Kafka producer gracefully."""
        if self.producer:
            self.producer.close()
            logging.info("Kafka producer closed.")

if __name__ == "__main__":
    # Initialize and run the log streamer
    try:
        streamer = LogStreamer()
        logging.info("Starting log streaming...")
        streamer.stream_logs(min_delay=1, max_delay=3)
    except Exception as e:
        logging.error(f"Fatal error: {e}")