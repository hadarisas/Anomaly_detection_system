from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import Chroma
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
            # Check for error keywords
            is_error = any(keyword in log.lower() for keyword in 
                         ['error', 'failed', 'failure', 'warning', 'critical', 'exception', 'fatal'])
            
            try:
                # Get sentiment score
                classification = self.anomaly_detector(log)[0]
                sentiment_score = classification['score'] if classification['label'] == 'NEGATIVE' else 1 - classification['score']
                
                # Combine heuristic and model-based detection
                if is_error or sentiment_score > 0.7:
                    anomalies.append({
                        "text": log,
                        "score": sentiment_score if is_error else sentiment_score * 0.8,
                        "timestamp": datetime.now().isoformat()
                    })
            except Exception as e:
                print(f"Error processing log: {e}")
                continue
                
        return anomalies 