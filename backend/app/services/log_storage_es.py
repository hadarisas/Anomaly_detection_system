from elasticsearch import AsyncElasticsearch
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import json
import re

class ElasticLogStorage:
    def __init__(self):
        self.es = AsyncElasticsearch(['http://localhost:9200'])
        self.raw_logs_index = 'raw-logs'
        self.anomalies_index = 'anomalies'
        
    async def initialize(self):
        """Initialize Elasticsearch indices"""
        try:
            # Raw logs mapping
            if not await self.es.indices.exists(index=self.raw_logs_index):
                await self.es.indices.create(
                    index=self.raw_logs_index,
                    mappings={
                        "properties": {
                            "timestamp": {"type": "date"},
                            "content": {"type": "text"},
                            "log_level": {"type": "keyword"},
                            "source": {"type": "keyword"},
                            "component": {"type": "keyword"},
                            "host": {"type": "keyword"},
                            "thread": {"type": "keyword"},
                            "transaction_id": {"type": "keyword"}
                        }
                    }
                )

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
                            "sub_type": {"type": "keyword"},
                            "duration_ms": {"type": "long"},
                            "source_component": {"type": "keyword"},
                            "stack_trace": {"type": "text"},
                            "classification": {
                                "properties": {
                                    "category": {"type": "keyword"},
                                    "confidence": {"type": "float"},
                                    "scores": {
                                        "properties": {
                                            "PERFORMANCE": {"type": "float"},
                                            "SECURITY": {"type": "float"},
                                            "AVAILABILITY": {"type": "float"},
                                            "DATA": {"type": "float"},
                                            "NETWORK": {"type": "float"},
                                            "RESOURCE": {"type": "float"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                )
            
        except Exception as e:
            raise e

    async def store_raw_logs(self, logs: List[str]):
        """Store raw logs to Elasticsearch"""
        bulk_data = []
        for log in logs:
            bulk_data.append({
                "index": {"_index": self.raw_logs_index}
            })
            timestamp = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=1))).isoformat()
            bulk_data.append({
                "@timestamp": timestamp,
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
                
                # Use current time (UTC+1) as detection timestamp
                timestamp = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=1))).isoformat()
                
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
                if response.get("errors"):
                    print(f"Errors during bulk indexing: {response}")
                return response
            return {"message": "No anomalies to index"}
            
        except Exception as e:
            print(f"Error storing anomalies: {e}")
            raise e

    async def get_recent_logs(self, limit: int = 15) -> List[Dict]:
        """Retrieve recent raw logs"""
        result = await self.es.search(
            index=self.anomalies_index,
            body={
                "query": {"match_all": {}},
                "sort": [{"@timestamp": "desc"}],
                "size": limit
            }
        )
        return [
            {
                "timestamp": hit["_source"]["@timestamp"],
                "text": hit["_source"]["text"],
                "score": hit["_source"]["score"],
                "type": hit["_source"]["type"],
                "sub_type": hit["_source"].get("sub_type", ""),
                "source_component": hit["_source"].get("source_component", ""),
                "duration_ms": hit["_source"].get("duration_ms", None)
            } 
            for hit in result["hits"]["hits"]
        ]

    def _parse_time_unit(self, time_unit: str = "5min") -> str:
        """Convert friendly time unit to Elasticsearch interval format"""
        time_map = {
            "1min": "1m",
            "5min": "5m",
            "10min": "10m",
            "20min": "20m",
            "30min": "30m",
            "1h": "1h",
            "24h": "24h"
        }
        return time_map.get(time_unit, "5m")

    def _get_severity(self, score: float) -> str:
        """Determine severity based on anomaly score"""
        return "critical" if score >= 0.75 else "warning"

    async def get_recent_anomalies(self, time_unit: str = "5min") -> Dict:
        """Retrieve recent anomalies"""
        try:
            interval = self._parse_time_unit(time_unit)
            
            # Get current time in UTC+1
            now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=1)))
            
            # Calculate start time based on time_unit
            if time_unit == "24h":
                start_time = now - timedelta(days=1)
            elif time_unit.endswith("h"):
                hours = int(time_unit[:-1])
                start_time = now - timedelta(hours=hours)
            else:
                minutes = int(time_unit[:-3])
                start_time = now - timedelta(minutes=minutes)

            print(f"Querying anomalies from {start_time} to {now}")
            query = {
                "size": 0,
                "query": {
                    "range": {
                        "@timestamp": {
                            "gte": start_time.isoformat(),
                            "lte": now.isoformat()
                        }
                    }
                },
                "aggs": {
                    "anomalies_over_time": {
                        "date_histogram": {
                            "field": "@timestamp",
                            "fixed_interval": "1m",
                            "min_doc_count": 0,
                            "extended_bounds": {
                                "min": start_time.isoformat(),
                                "max": now.isoformat()
                            }
                        },
                        "aggs": {
                            "severity": {
                                "range": {
                                    "field": "score",
                                    "ranges": [
                                        { "from": 0.0, "to": 0.75, "key": "warning" },
                                        { "from": 0.75, "to": 1.0, "key": "critical" }
                                    ]
                                }
                            }
                        }
                    },
                    "total_by_severity": {
                        "range": {
                            "field": "score",
                            "ranges": [
                                { "from": 0.0, "to": 0.75, "key": "warning" },
                                { "from": 0.75, "to": 1.0, "key": "critical" }
                            ]
                        }
                    }
                }
            }

           
            result = await self.es.search(
                index=self.anomalies_index,
                body=query
            )
            
            # Get total counts
            totals = {
                "critical": 0,
                "warning": 0
            }
            for bucket in result.body["aggregations"]["total_by_severity"]["buckets"]:
                totals[bucket["key"]] = bucket["doc_count"]

            return {
                "totals": totals,
                "query_details": {
                    "start_time": start_time.isoformat(),
                    "end_time": now.isoformat(),
                    "current_time": now.isoformat(),  
                    "last_anomaly_time": result.body["aggregations"]["anomalies_over_time"]["buckets"][0]["key_as_string"] if result.body["aggregations"]["anomalies_over_time"]["buckets"] else None,
                    "interval": interval
                }
            }

        except Exception as e:
            print(f"Error in get_recent_anomalies: {str(e)}")
            return {
                "totals": {"critical": 0, "warning": 0},
                "error": str(e)
            }

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
        """Determine anomaly type and map to high-level categories
        Categories: PERFORMANCE, SECURITY, AVAILABILITY, DATA, NETWORK, RESOURCE
        """
        base_info = {
            "source_component": self._extract_component(text)
        }

        # PERFORMANCE Category
        if "JvmPauseMonitor" in text:
            duration = int(''.join(filter(str.isdigit, text.split("approximately")[1].split("ms")[0])))
            return {
                **base_info,
                "type": "PERFORMANCE",
                "sub_type": "JVM_PAUSE",
                "duration_ms": duration,
            }
        elif "Slow BlockReceiver" in text or "slow io" in text.lower():
            return {
                **base_info,
                "type": "PERFORMANCE",
                "sub_type": "SLOW_IO"
            }

        # AVAILABILITY Category
        elif "NameNode" in text:
            if "Failed to start active state service" in text:
                return {
                    **base_info,
                    "type": "AVAILABILITY",
                    "sub_type": "STARTUP_FAILURE",
                }
            elif "Lost leadership" in text:
                return {
                    **base_info,
                    "type": "AVAILABILITY",
                    "sub_type": "LEADERSHIP_LOSS",
                }

        # DATA Category
        elif "BlockManager" in text:
            if "corrupted" in text:
                return {
                    **base_info,
                    "type": "DATA",
                    "sub_type": "CORRUPTION",
                }
            elif "Unable to place replica" in text:
                return {
                    **base_info,
                    "type": "DATA",
                    "sub_type": "REPLICA_PLACEMENT",
                }
            elif "Total load" in text:
                return {
                    **base_info,
                    "type": "RESOURCE",
                    "sub_type": "HIGH_LOAD",
                }

        # SECURITY Category
        elif "Login failed" in text or "authentication failed" in text:
            return {
                **base_info,
                "type": "SECURITY",
                "sub_type": "LOGIN_FAILURE" if "Login failed" in text else "AUTH_FAILURE",
            }
        elif "Token" in text and "ERROR" in text:
            return {
                **base_info,
                "type": "SECURITY",
                "sub_type": "TOKEN_ERROR"
            }

        # NETWORK Category
        elif "Connection timed out" in text:
            return {
                **base_info,
                "type": "NETWORK",
                "sub_type": "TIMEOUT",
            }
        elif "Network error" in text or "Connection refused" in text:
            return {
                **base_info,
                "type": "NETWORK",
                "sub_type": "CONNECTION_ERROR"
            }

        # RESOURCE Category
        elif "disk space" in text.lower() or "capacity exceeded" in text:
            return {
                **base_info,
                "type": "RESOURCE",
                "sub_type": "DISK_SPACE"
            }
        elif "Memory usage" in text or "OutOfMemoryError" in text:
            return {
                **base_info,
                "type": "RESOURCE",
                "sub_type": "MEMORY"
            }

        # Default/Unknown case
        return {
            **base_info,
            "type": "UNKNOWN",
            "sub_type": "GENERAL"
        }

    def _extract_component(self, text: str) -> str:
        """Extract the component from the log message"""
        component_match = re.search(r'org\.apache\.hadoop\.([\w\.]+):', text)
        return component_match.group(1) if component_match else "unknown"

    async def close(self):
        """Close Elasticsearch connection"""
        await self.es.close()

    async def get_anomaly_history(self, start_date: str = None, end_date: str = None, interval: str = "1h"):
        """Get historical anomaly totals by category"""
        try:
            # If no dates provided, get all data
            if not start_date or not end_date:
                # Get the earliest and latest timestamps from the index
                earliest = await self.es.search(
                    index=self.anomalies_index,
                    body={
                        "size": 1,
                        "sort": [{"@timestamp": "asc"}],
                        "_source": ["@timestamp"]
                    }
                )
                latest = await self.es.search(
                    index=self.anomalies_index,
                    body={
                        "size": 1,
                        "sort": [{"@timestamp": "desc"}],
                        "_source": ["@timestamp"]
                    }
                )
                
                if earliest["hits"]["hits"] and latest["hits"]["hits"]:
                    start_date = earliest["hits"]["hits"][0]["_source"]["@timestamp"]
                    end_date = latest["hits"]["hits"][0]["_source"]["@timestamp"]
                else:
                    # If no data exists, use a default range
                    now = datetime.now(timezone.utc)
                    end_date = now.isoformat()
                    start_date = (now - timedelta(days=30)).isoformat()

            # Base query without time range
            query = {
                "size": 0,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": start_date,
                                        "lte": end_date
                                    }
                                }
                            }
                        ]
                    }
                },
                "aggs": {
                    "total_by_category": {
                        "terms": {
                            "field": "type",
                            "include": [
                                "PERFORMANCE",
                                "SECURITY", 
                                "AVAILABILITY",
                                "DATA",
                                "NETWORK",
                                "RESOURCE",
                                "UNKNOWN"
                            ],
                            "min_doc_count": 0
                        }
                    }
                }
            }

            result = await self.es.search(
                index=self.anomalies_index,
                body=query
            )
            
            result_dict = result.body
            
            # Get total counts
            totals = {
                "PERFORMANCE": 0,
                "SECURITY": 0,
                "AVAILABILITY": 0,
                "DATA": 0,
                "NETWORK": 0,
                "RESOURCE": 0,
                "UNKNOWN": 0
            }
            for bucket in result_dict["aggregations"]["total_by_category"]["buckets"]:
                totals[bucket["key"]] = bucket["doc_count"]

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