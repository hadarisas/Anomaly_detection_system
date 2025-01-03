from elasticsearch import AsyncElasticsearch
from datetime import datetime
from typing import List, Dict
import json

class ElasticLogStorage:
    def __init__(self):
        self.es = AsyncElasticsearch(['http://localhost:9200'])
        self.raw_logs_index = 'raw-logs'
        self.anomalies_index = 'anomalies'
        
    async def initialize(self):
        """Initialize Elasticsearch indices"""
        # Raw logs mapping
        if not await self.es.indices.exists(index=self.raw_logs_index):
            await self.es.indices.create(
                index=self.raw_logs_index,
                mappings={
                    "properties": {
                        "timestamp": {"type": "date"},
                        "content": {"type": "text"},
                        "log_level": {"type": "keyword"},
                        "source": {"type": "keyword"}
                    }
                }
            )

        # Anomalies mapping
        if not await self.es.indices.exists(index=self.anomalies_index):
            await self.es.indices.create(
                index=self.anomalies_index,
                mappings={
                    "properties": {
                        "timestamp": {"type": "date"},
                        "text": {"type": "text"},
                        "score": {"type": "float"},
                        "type": {
                            "type": "keyword"
                        }
                    }
                }
            )

    async def store_raw_logs(self, logs: List[str]):
        """Store raw logs to Elasticsearch"""
        bulk_data = []
        for log in logs:
            bulk_data.append({
                "index": {"_index": self.raw_logs_index}
            })
            bulk_data.append({
                "timestamp": datetime.now().isoformat(),
                "content": log,
                "log_level": self._extract_log_level(log),
                "source": self._extract_source(log)
            })
        
        await self.es.bulk(operations=bulk_data, refresh=True)

    async def store_anomalies(self, anomalies: List[Dict]):
        """Store detected anomalies to Elasticsearch"""
        bulk_data = []
        for anomaly in anomalies:
            bulk_data.append({
                "index": {"_index": self.anomalies_index}
            })
            bulk_data.append({
                "timestamp": anomaly.get("timestamp", datetime.now().isoformat()),
                "text": anomaly["text"],
                "score": anomaly["score"],
                "type": self._determine_anomaly_type(anomaly["text"])
            })
        
        await self.es.bulk(operations=bulk_data, refresh=True)

    async def get_recent_logs(self, limit: int = 100) -> List[str]:
        """Retrieve recent raw logs"""
        result = await self.es.search(
            index=self.raw_logs_index,
            body={
                "query": {"match_all": {}},
                "sort": [{"timestamp": "desc"}],
                "size": limit
            }
        )
        return [hit["_source"]["content"] for hit in result["hits"]["hits"]]

    async def get_recent_anomalies(self, limit: int = 50) -> List[Dict]:
        """Retrieve recent anomalies"""
        result = await self.es.search(
            index=self.anomalies_index,
            body={
                "query": {"match_all": {}},
                "sort": [{"timestamp": "desc"}],
                "size": limit
            }
        )
        return [hit["_source"] for hit in result["hits"]["hits"]]

    def _extract_log_level(self, log: str) -> str:
        """Extract log level from log message"""
        if "ERROR" in log:
            return "ERROR"
        elif "WARN" in log:
            return "WARN"
        elif "INFO" in log:
            return "INFO"
        elif "FATAL" in log:
            return "FATAL"
        return "UNKNOWN"

    def _extract_source(self, log: str) -> str:
        """Extract source from log message"""
        if "DataNode" in log:
            return "DataNode"
        elif "NameSystem" in log:
            return "NameSystem"
        return "Other"

    def _determine_anomaly_type(self, text: str) -> str:
        """Determine the type of anomaly based on the log text"""
        if "IOException" in text:
            return "IO_ERROR"
        elif "Slow BlockReceiver" in text:
            return "PERFORMANCE"
        elif "corrupted" in text:
            return "DATA_CORRUPTION"
        elif "Exception" in text:
            return "GENERAL_ERROR"
        return "UNKNOWN"

    async def close(self):
        """Close Elasticsearch connection"""
        await self.es.close()

    async def get_anomaly_history(self, start_date=None, end_date=None, interval="1h"):
        """Get historical anomaly data with aggregations"""
        try:
            # Get the timestamp range from the data
            range_query = {
                "size": 0,
                "aggs": {
                    "min_time": { "min": { "field": "timestamp" } },
                    "max_time": { "max": { "field": "timestamp" } }
                }
            }
            
            range_result = await self.es.search(
                index=self.anomalies_index,
                body=range_query
            )
            
            # Use provided dates or fall back to data range
            query = {
                "size": 0,
                "query": {
                    "range": {
                        "timestamp": {
                            "gte": start_date if start_date else "now-24h",
                            "lte": end_date if end_date else "now"
                        }
                    }
                },
                "aggs": {
                    "anomalies_over_time": {
                        "date_histogram": {
                            "field": "timestamp",
                            "fixed_interval": interval,
                            "min_doc_count": 0
                        },
                        "aggs": {
                            "by_type": {
                                "terms": {
                                    "field": "type",
                                    "size": 10
                                }
                            }
                        }
                    }
                }
            }

            result = await self.es.search(
                index=self.anomalies_index,
                body=query
            )
            
            buckets = result.body["aggregations"]["anomalies_over_time"]["buckets"]
            transformed_data = []
            
            for bucket in buckets:
                if bucket["doc_count"] > 0:  # Only include non-empty buckets
                    data_point = {
                        "time": bucket["key_as_string"],
                        "count": bucket["doc_count"]
                    }
                    
                    # Add counts for each type
                    type_counts = {
                        "IO_ERROR": 0,
                        "DATA_CORRUPTION": 0,
                        "PERFORMANCE": 0,
                        "UNKNOWN": 0
                    }
                    
                    for type_bucket in bucket["by_type"]["buckets"]:
                        type_counts[type_bucket["key"]] = type_bucket["doc_count"]
                    
                    data_point.update(type_counts)
                    transformed_data.append(data_point)
            
            return transformed_data

        except Exception as e:
            print(f"Error in get_anomaly_history: {e}")
            raise 