from elasticsearch import AsyncElasticsearch
from datetime import datetime, timedelta
from typing import List, Dict
import json

class ElasticLogStorage:
    def __init__(self):
        self.es = AsyncElasticsearch(['http://localhost:9200'])
        self.raw_logs_index = 'raw-logs'
        self.anomalies_index = 'anomalies'
        
    async def initialize(self):
        """Initialize Elasticsearch indices"""
        try:
            # Anomalies mapping with enhanced categories
            if not await self.es.indices.exists(index=self.anomalies_index):
                await self.es.indices.create(
                    index=self.anomalies_index,
                    mappings={
                        "properties": {
                            "@timestamp": {"type": "date"},
                            "text": {"type": "text"},
                            "score": {"type": "float"},
                            "type": {"type": "keyword"},
                            "sub_type": {"type": "keyword"},  # New field for detailed categorization
                            "duration_ms": {"type": "long"},  # For JVM pauses
                            "source_component": {"type": "keyword"},  # Component that generated the error
                            "stack_trace": {"type": "text"}  # For storing stack traces
                        }
                    }
                )
                print(f"Created index {self.anomalies_index}")
            
            # Keep existing debug prints
            indices = await self.es.indices.get(index="*")
            print(f"Existing indices: {indices}")
            mapping = await self.es.indices.get_mapping(index=self.anomalies_index)
            print(f"Index mapping: {mapping}")
            
        except Exception as e:
            print(f"Error initializing ES: {e}")
            raise e

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
        """Store detected anomalies with enhanced metadata"""
        try:
            bulk_data = []
            for anomaly in anomalies:
                bulk_data.append({
                    "index": {"_index": self.anomalies_index}
                })
                
                # Ensure timestamp is in ISO format
                timestamp = anomaly.get("timestamp", datetime.now().isoformat())
                if isinstance(timestamp, datetime):
                    timestamp = timestamp.isoformat()
                
                # Get detailed anomaly type info
                anomaly_info = self._determine_anomaly_type(anomaly["text"])
                
                # Extract stack trace if present
                stack_trace = None
                if "\n\t" in anomaly["text"]:
                    stack_trace = "\n".join(anomaly["text"].split("\n")[1:])
                
                # Combine original data with enhanced metadata
                doc = {
                    "@timestamp": timestamp,
                    "text": anomaly["text"],
                    "score": anomaly["score"],
                    "type": anomaly_info["type"],
                    "sub_type": anomaly_info.get("sub_type"),
                    "duration_ms": anomaly_info.get("duration_ms"),
                    "source_component": anomaly_info.get("source_component"),
                    "stack_trace": stack_trace
                }
                
                # Remove None values to keep documents clean
                doc = {k: v for k, v in doc.items() if v is not None}
                bulk_data.append(doc)
            
            if bulk_data:
                response = await self.es.bulk(operations=bulk_data, refresh=True)
                print(f"Bulk indexing response: {response}")
                if response.get("errors"):
                    print(f"Errors during bulk indexing: {response}")
                return response
            return {"message": "No anomalies to index"}
            
        except Exception as e:
            print(f"Error storing anomalies: {e}")
            raise e

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

    def _determine_anomaly_type(self, text: str) -> dict:
        """Enhanced anomaly type detection with sub-categories"""
        if "JvmPauseMonitor" in text:
            duration = int(''.join(filter(str.isdigit, text.split("approximately")[1].split("ms")[0])))
            severity = "WARN" if duration > 15000 else "INFO"
            return {
                "type": "JVM_PAUSE",
                "sub_type": f"{severity}_PAUSE",
                "duration_ms": duration,
                "source_component": "JvmPauseMonitor"
            }
        elif "Connection timed out" in text:
            return {
                "type": "CONNECTION_ERROR",
                "sub_type": "TIMEOUT",
                "source_component": "Server" if "Socket Reader" in text else "Other"
            }
        elif "NullPointerException" in text:
            return {
                "type": "THREAD_ERROR",
                "sub_type": "NULL_POINTER",
                "source_component": text.split("at ")[1].split(".")[0] if "at " in text else "Unknown"
            }
        elif "Token" in text and "ERROR" in text:
            return {
                "type": "SECURITY_ERROR",
                "sub_type": "TOKEN_ERROR",
                "source_component": "SecurityManager"
            }
        elif "DataNode" in text and "ERROR" in text:
            if "corrupted" in text:
                return {"type": "DATA_CORRUPTION", "sub_type": "BLOCK_CORRUPT"}
            else:
                return {"type": "DATANODE_ERROR", "sub_type": "GENERAL_ERROR"}
        
        # Maintain backward compatibility with original types
        elif "IOException" in text:
            return {"type": "IO_ERROR"}
        elif "Slow BlockReceiver" in text:
            return {"type": "PERFORMANCE"}
        elif "Exception" in text:
            return {"type": "GENERAL_ERROR"}
            
        return {"type": "UNKNOWN"}

    async def close(self):
        """Close Elasticsearch connection"""
        await self.es.close()

    async def get_anomaly_history(self, start_date: str = None, end_date: str = None, interval: str = "1h"):
        """Get historical anomaly totals"""
        try:
            # Base query without time range
            query = {
                "size": 0,
                "aggs": {
                    "total_unknown": {
                        "filter": {
                            "term": {
                                "type": "UNKNOWN"
                            }
                        }
                    },
                    "total_io_error": {
                        "filter": {
                            "term": {
                                "type": "IO_ERROR"
                            }
                        }
                    },
                    "total_data_corruption": {
                        "filter": {
                            "term": {
                                "type": "DATA_CORRUPTION"
                            }
                        }
                    },
                    "total_performance": {
                        "filter": {
                            "term": {
                                "type": "PERFORMANCE"
                            }
                        }
                    }
                }
            }

            # Only add time range and timestamp existence check if dates are provided
            if start_date and end_date:
                query["query"] = {
                    "bool": {
                        "must": [
                            {
                                "bool": {
                                    "should": [
                                        {"exists": {"field": "timestamp"}},
                                        {"exists": {"field": "@timestamp"}}
                                    ],
                                    "minimum_should_match": 1
                                }
                            },
                            {
                                "bool": {
                                    "should": [
                                        {
                                            "range": {
                                                "timestamp": {
                                                    "gte": start_date,
                                                    "lte": end_date
                                                }
                                            }
                                        },
                                        {
                                            "range": {
                                                "@timestamp": {
                                                    "gte": start_date,
                                                    "lte": end_date
                                                }
                                            }
                                        }
                                    ],
                                    "minimum_should_match": 1
                                }
                            }
                        ]
                    }
                }

            print(f"ES Query: {json.dumps(query, indent=2)}")
            result = await self.es.search(
                index=self.anomalies_index,
                body=query
            )
            
            result_dict = result.body
            print(f"ES Response: {json.dumps(result_dict, indent=2)}")

            totals = {
                "io_error": result_dict["aggregations"]["total_io_error"]["doc_count"],
                "data_corruption": result_dict["aggregations"]["total_data_corruption"]["doc_count"],
                "performance": result_dict["aggregations"]["total_performance"]["doc_count"],
                "unknown": result_dict["aggregations"]["total_unknown"]["doc_count"]
            }

            return {
                "totals": totals,
                "total_anomalies": sum(totals.values()),
                "query_details": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "index": self.anomalies_index
                }
            }

        except Exception as e:
            print(f"Error in get_anomaly_history: {str(e)}")
            return {"error": str(e)}

    # Add a debug method to check index contents
    async def debug_index_contents(self):
        """Debug method to check all documents in the index"""
        try:
            result = await self.es.search(
                index=self.anomalies_index,
                body={
                    "query": {"match_all": {}},
                    "size": 100  # Adjust size as needed
                }
            )
            print("Index contents:")
            print(json.dumps(result, indent=2))
            return result
        except Exception as e:
            print(f"Error checking index contents: {e}")
            return {"error": str(e)} 