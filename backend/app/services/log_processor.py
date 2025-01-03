from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

class LogProcessor:
    def __init__(self):
        # Initialize with Hugging Face API token
        self.hf_api_token = os.getenv('HUGGING_FACE_API_TOKEN')
        if not self.hf_api_token:
            raise ValueError("HUGGING_FACE_API_TOKEN environment variable is required")

        self.embeddings = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2",
            model_kwargs={'token': self.hf_api_token}
        )
        
        # Use a general-purpose text classification model
        self.anomaly_detector = pipeline(
            "text-classification",
            model="distilbert-base-uncased-finetuned-sst-2-english",
            token=self.hf_api_token
        )
        self.vector_store = None
        
    async def process_logs(self, log_content: str):
        if not log_content:
            return []

        # Split logs into lines
        logs = log_content.strip().split('\n')
        
        # Detect anomalies
        anomalies = []
        for log in logs:
            # First, determine the log level
            log_level = self._extract_log_level(log)
            
            # Only process WARN, ERROR, and FATAL logs as potential anomalies
            if log_level in ["WARN", "ERROR", "FATAL"]:
                try:
                    # Get sentiment score for additional context
                    classification = self.anomaly_detector(log)[0]
                    sentiment_score = classification['score'] if classification['label'] == 'NEGATIVE' else 1 - classification['score']
                    
                    # Adjust score based on log level
                    final_score = self._calculate_anomaly_score(log_level, sentiment_score)
                    
                    # Add to anomalies if score meets threshold
                    if final_score > 0.5:  # Adjusted threshold
                        anomalies.append({
                            "text": log,
                            "score": final_score,
                            "timestamp": datetime.now().isoformat(),
                            "level": log_level
                        })
                except Exception as e:
                    print(f"Error processing log: {e}")
                    continue
                    
        return anomalies

    def _extract_log_level(self, log: str) -> str:
        """Extract log level from log message"""
        if "FATAL" in log:
            return "FATAL"
        elif "ERROR" in log:
            return "ERROR"
        elif "WARN" in log:
            return "WARN"
        elif "INFO" in log:
            return "INFO"
        return "UNKNOWN"

    def _calculate_anomaly_score(self, log_level: str, sentiment_score: float) -> float:
        """Calculate final anomaly score based on log level and sentiment"""
        base_scores = {
            "FATAL": 1.0,
            "ERROR": 0.8,
            "WARN": 0.6,
            "INFO": 0.0,  # INFO logs get a base score of 0
            "UNKNOWN": 0.5
        }
        
        # Combine base score with sentiment
        base_score = base_scores.get(log_level, 0.5)
        return (base_score * 0.7) + (sentiment_score * 0.3)  # Weight more towards log level 