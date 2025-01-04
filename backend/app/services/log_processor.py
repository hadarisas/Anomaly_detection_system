from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import os
from dotenv import load_dotenv
from datetime import datetime
import re
import torch
from .email_service import EmailService
import warnings
from transformers import logging as transformers_logging

load_dotenv()

# Suppress specific model warnings
warnings.filterwarnings("ignore", message="Some weights of RobertaForSequenceClassification were not initialized")
warnings.filterwarnings("ignore", message="You should probably TRAIN this model on a down-stream task")
transformers_logging.set_verbosity_error()  # Only show errors, not warnings

# Define anomaly categories and their descriptions for the model
ANOMALY_CATEGORIES = {
    "PERFORMANCE": "Issues related to system speed, response time, or resource usage efficiency",
    "SECURITY": "Security breaches, authentication failures, or unauthorized access attempts",
    "AVAILABILITY": "System downtime, service interruptions, or accessibility issues",
    "DATA": "Data corruption, integrity issues, or data loss scenarios",
    "NETWORK": "Network connectivity, timeout, or communication problems",
    "RESOURCE": "CPU, memory, or disk space utilization issues"
}

class LogProcessor:
    def __init__(self):
        warnings.filterwarnings("ignore", message="Device set to use cpu")
        # Initialize  Hugging Face API token
        self.hf_api_token = os.getenv('HUGGING_FACE_API_TOKEN')
        if not self.hf_api_token:
            raise ValueError("HUGGING_FACE_API_TOKEN environment variable is required")

        # Initialize email service
        self.email_service = EmailService()

        # Define RFC 5424 standard scores
        self.SEVERITY_SCORES = {
            "FATAL": 1.0,     # Maps to EMERG/ALERT (0-1 in RFC 5424)
            "ERROR": 0.75,    # Maps to ERROR (3 in RFC 5424)
            "WARN": 0.5,      # Maps to WARNING (4 in RFC 5424)
            "INFO": 0.25,     # Maps to INFO (6 in RFC 5424)
            "UNKNOWN": 0.25   # Default to INFO level
        }

        # Embeddings model
        self.embeddings = HuggingFaceEmbeddings(
            model_name="BAAI/bge-small-en-v1.5",  
            model_kwargs={
                'token': self.hf_api_token,
                'device': 'cuda' if torch.cuda.is_available() else 'cpu'  # GPU acceleration if available
            }
        )
        
        # Anomaly detection model
        self.anomaly_detector = pipeline(
            "text-classification",
            model="roberta-base",  
            token=self.hf_api_token,
            ignore_mismatched_sizes=True,
            device=0 if torch.cuda.is_available() else -1  # GPU acceleration if available
        )

        # Anomaly classification model
        self.anomaly_classifier = pipeline(
            "zero-shot-classification",
            model="microsoft/codebert-base-mlm",
            token=self.hf_api_token,
            ignore_mismatched_sizes=True,
            model_kwargs={"label2id": {"ENTAILMENT": 0, "NOT_ENTAILMENT": 1}},
            device=0 if torch.cuda.is_available() else -1
        )
        
        # Prepare category labels and descriptions
        self.category_labels = list(ANOMALY_CATEGORIES.keys())
        self.category_descriptions = [f"{k}: {v}" for k, v in ANOMALY_CATEGORIES.items()]

        self.vector_store = None
        
    def _extract_log_level_and_component(self, log: str) -> tuple:
        """Extract log level and component based on RFC 5424 standards"""
        for level in self.SEVERITY_SCORES:
            if level in log:
                base_score = self.SEVERITY_SCORES[level]
                break
        else:
            base_score = self.SEVERITY_SCORES["UNKNOWN"]
            
        # Extract component
        component_match = re.search(r'org\.apache\.hadoop\.([\w\.]+):', log)
        component = component_match.group(1) if component_match else "unknown"
        
        return base_score, component

    def _parse_jvm_pause(self, log: str) -> dict:
        """Parse JVM pause information"""
        if "JvmPauseMonitor" in log and "pause" in log:
            try:
                duration = int(''.join(filter(str.isdigit, log.split("approximately")[1].split("ms")[0])))
                return {
                    "type": "JVM_PAUSE",
                    "duration_ms": duration,
                    "severity": "HIGH" if duration > 15000 else "MEDIUM" if duration > 5000 else "LOW"
                }
            except:
                pass
        return None

    def _parse_stack_trace(self, log: str) -> dict:
        """Parse stack trace information"""
        if "\n\t" in log or "\nat " in log:
            lines = log.split("\n")
            exception_type = lines[0].split(": ")[-1] if ": " in lines[0] else "Unknown Exception"
            return {
                "type": "EXCEPTION",
                "exception_type": exception_type,
                "stack_trace": "\n".join(lines[1:])
            }
        return None

    def _calculate_anomaly_score(self, log_level: str, sentiment_score: float, additional_factors: dict = None) -> float:
        """Enhanced anomaly score calculation using RFC 5424 standards"""
        base_score = self.SEVERITY_SCORES.get(log_level, self.SEVERITY_SCORES["UNKNOWN"])
        
        # Adjust score based on additional factors
        if additional_factors:
            if "JVM_PAUSE" in additional_factors.get("type", ""):
                duration_ms = additional_factors.get("duration_ms", 0)
                if duration_ms > 15000:  # Severe pause
                    base_score = max(base_score, 0.9)
                elif duration_ms > 5000:  # Moderate pause
                    base_score = max(base_score, 0.7)
            
            if "EXCEPTION" in additional_factors.get("type", ""):
                if "NullPointerException" in additional_factors.get("exception_type", ""):
                    base_score = max(base_score, self.SEVERITY_SCORES["ERROR"])
                elif "IOException" in additional_factors.get("exception_type", ""):
                    base_score = max(base_score, self.SEVERITY_SCORES["WARN"])

        # Combine with sentiment score (keeping original weighting for compatibility)
        return (base_score * 0.7) + (sentiment_score * 0.3)

    async def classify_anomaly(self, text: str) -> dict:
        """Classify anomaly into predefined categories"""
        try:
            # Anomaly classification
            result = self.anomaly_classifier(
                text,
                candidate_labels=self.category_descriptions,
                hypothesis_template="This log entry describes {}"
            )
            
            # Get the highest scoring category
            best_match_idx = result['scores'].index(max(result['scores']))
            category = self.category_labels[best_match_idx]
            confidence = result['scores'][best_match_idx]
            
            return {
                "category": category,
                "confidence": confidence,
                "scores": dict(zip(self.category_labels, result['scores']))
            }
        except Exception as e:
            print(f"Error in anomaly classification: {e}")
            return {"category": "UNKNOWN", "confidence": 0.0}

    async def process_logs(self, log_content: str):
        """Process logs with anomaly detection and classification"""
        if not log_content:
            return []

        # Split logs into lines
        logs = log_content.strip().split('\n')
        
        # Detect anomalies
        anomalies = []
        for log in logs:
            try:
                # Extract basic information
                base_score, component = self._extract_log_level_and_component(log)
                
                # Check for specific patterns
                jvm_info = self._parse_jvm_pause(log)
                stack_trace_info = self._parse_stack_trace(log)
                
                # Get sentiment score for additional context
                classification = self.anomaly_detector(log)[0]
                sentiment_score = classification['score'] if classification['label'] == 'NEGATIVE' else 1 - classification['score']
                
                # Calculate final score with additional context
                additional_factors = {**jvm_info} if jvm_info else {**stack_trace_info} if stack_trace_info else {}
                final_score = self._calculate_anomaly_score(
                    self._extract_log_level(log),
                    sentiment_score,
                    additional_factors
                )
                
                # Add to anomalies if score meets threshold
                if final_score > 0.5:  # Keeping original threshold for compatibility
                    anomaly = {
                        "text": log,
                        "score": final_score,
                        "timestamp": datetime.now().isoformat(),
                        "level": self._extract_log_level(log),
                        "component": component
                    }
                    
                    # Add additional information if available
                    if jvm_info:
                        anomaly.update(jvm_info)
                    if stack_trace_info:
                        anomaly.update(stack_trace_info)
                        
                    # Add classification for anomalies
                    classification_result = await self.classify_anomaly(log)
                    anomaly.update({
                        "classification": classification_result
                    })
                    
                    # Send email notification for high-severity anomalies
                    if final_score > 0.7:  # High severity threshold
                        admin_emails = self.email_service.get_admin_emails(classification_result['category'])
                        
                        if admin_emails:
                            subject, body = self.email_service.format_anomaly_notification(
                                anomaly_text=log,
                                anomaly_type=classification_result['category'],
                                score=final_score,
                                timestamp=datetime.now()
                            )
                            
                            await self.email_service.send_email(
                                subject=subject,
                                body=body,
                                to_emails=admin_emails,
                                anomaly_type=classification_result['category']
                            )
                        
                    anomalies.append(anomaly)
                    
            except Exception as e:
                print(f"Error processing log: {e}")
                continue
                    
        return anomalies

    def _extract_log_level(self, log: str) -> str:
        """Keep existing method for backward compatibility"""
        if "FATAL" in log:
            return "FATAL"
        elif "ERROR" in log:
            return "ERROR"
        elif "WARN" in log:
            return "WARN"
        elif "INFO" in log:
            return "INFO"
        return "UNKNOWN"
        