import logging
from kafka import KafkaConsumer
import json
import threading
import os
from dotenv import load_dotenv
import asyncio
from pathlib import Path
from datetime import datetime
from app.services.log_storage_es import ElasticLogStorage
from app.services.log_processor import LogProcessor
import time
# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaLogConsumer:
    def __init__(self, bootstrap_servers='localhost:29092', topic='hadoop-logs'):
        """Initialize the Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='latest',
                enable_auto_commit=False,
                group_id='fastapi-log-consumer',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            self._stop_event = threading.Event()
            self._thread = None
            
            # Initialize storage and processor
            self.log_storage = ElasticLogStorage()
            self.log_processor = LogProcessor()
            
            # Set up log directory
            self.log_dir = Path("logs/raw")
            self.log_dir.mkdir(parents=True, exist_ok=True)
            self.current_log_file = None
            self.init_log_file()
            
            print("✅ Kafka consumer initialized successfully")
            self._running = False
        except Exception as e:
            print(f"❌ Failed to initialize Kafka consumer: {e}")
            raise

    def init_log_file(self):
        """Initialize a new log file for the current day"""
        current_date = datetime.now().strftime("%Y-%m-%d")
        self.current_log_file = self.log_dir / f"hdfs_{current_date}.log"

    def write_to_log_file(self, logs: list):
        """Write logs to the current day's log file"""
        try:
            # Check if we need a new log file (day changed)
            if not self.current_log_file or \
               self.current_log_file.stem != f"hdfs_{datetime.now().strftime('%Y-%m-%d')}":
                self.init_log_file()

            # Write logs to file
            with open(self.current_log_file, "a") as f:
                for log in logs:
                    f.write(f"{log}\n")
        except Exception as e:
            print(f"Error writing to log file: {e}")

    async def consume_logs(self, websocket):
        """Consume messages from Kafka and process them"""
        try:
            self._running = True
            
            # Send initial status
            if websocket:
                await websocket.send_json({
                    "type": "kafka_status",
                    "status": "running"
                })

            await self.log_storage.initialize()
            print("✅ Log storage initialized")
            
            while self._running:
                try:
                    messages = self.consumer.poll(timeout_ms=1000)
                    logs = []
                    
                    for topic_partition, msgs in messages.items():
                        for message in msgs:
                            try:
                                log_data = message.value
                                if isinstance(log_data, dict) and 'log' in log_data:
                                    logs.append(log_data['log'])
                            except Exception as e:
                                print(f"❌ Error processing message: {e}")
                                continue
                    
                    if logs:
                        self.write_to_log_file(logs)
                        await self.log_storage.store_raw_logs(logs)
                        anomalies = await self.log_processor.process_logs("\n".join(logs))
                        
                        if anomalies and websocket:
                            await self.log_storage.store_anomalies(anomalies)
                            print(f"🚨 Sending {len(anomalies)} anomalies through WebSocket")
                            # Format anomalies for frontend
                            formatted_anomalies = [{
                                "text": anomaly.get("log", ""),
                                "score": anomaly.get("score", 0),
                                "type": anomaly.get("type", "unknown")
                            } for anomaly in anomalies]
                            await websocket.send_json(formatted_anomalies)

                except Exception as e:
                    print(f"Error in consumer loop: {e}")
                    if not self._running:
                        break
                    await asyncio.sleep(1)

        except Exception as e:
            print(f"❌ Error in consumer: {e}")
        finally:
            self._running = False
            if websocket:
                await websocket.send_json({
                    "type": "kafka_status",
                    "status": "stopped"
                })

    def start(self):
        """Start the Kafka consumer in a separate thread"""
        if not self._thread:
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_async_consumer)
            self._thread.daemon = True
            self._thread.start()
            print("✅ Kafka consumer thread started")

    def _run_async_consumer(self):
        """Run the async consumer in a new event loop"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.consume_logs())
        finally:
            loop.close()

    async def stop(self):
        """Stop consuming messages"""
        self._running = False
        if self.consumer:
            self.consumer.close()
        print("✅ Kafka consumer stopped")

    def is_alive(self):
        """Check if the consumer thread is alive"""
        return self._thread and self._thread.is_alive()

    def is_running(self):
        """Check if consumer is running"""
        return self._running

if __name__ == "__main__":
    consumer = KafkaLogConsumer()
    try:
        consumer.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        consumer.stop()