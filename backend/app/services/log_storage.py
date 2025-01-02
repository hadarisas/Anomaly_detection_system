import os
from datetime import datetime
import json
import aiofiles
from typing import List, Dict

class LogStorage:
    def __init__(self):
        self.base_dir = "logs"
        self.ensure_directories()

    def ensure_directories(self):
        """Create necessary directories if they don't exist"""
        os.makedirs(self.base_dir, exist_ok=True)
        os.makedirs(os.path.join(self.base_dir, "raw"), exist_ok=True)
        os.makedirs(os.path.join(self.base_dir, "anomalies"), exist_ok=True)

    async def store_raw_logs(self, logs: List[str]):
        """Store raw logs to a file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_logs_{timestamp}.log"
        
        async with aiofiles.open(os.path.join(self.base_dir, "raw", filename), 'a') as f:
            await f.write("\n".join(logs) + "\n")

    async def store_anomalies(self, anomalies: List[Dict]):
        """Store detected anomalies to a JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"anomalies_{timestamp}.json"
        
        async with aiofiles.open(os.path.join(self.base_dir, "anomalies", filename), 'a') as f:
            await f.write(json.dumps(anomalies, indent=2) + "\n")

    async def get_recent_logs(self, limit: int = 100) -> List[str]:
        """Retrieve recent raw logs"""
        logs = []
        raw_dir = os.path.join(self.base_dir, "raw")
        
        if not os.path.exists(raw_dir):
            return logs

        files = sorted(os.listdir(raw_dir), reverse=True)
        for file in files[:5]:  # Look at 5 most recent files
            async with aiofiles.open(os.path.join(raw_dir, file), 'r') as f:
                content = await f.read()
                logs.extend(content.splitlines())
        
        return logs[-limit:]  # Return only the last 'limit' logs

    async def get_recent_anomalies(self, limit: int = 50) -> List[Dict]:
        """Retrieve recent anomalies"""
        anomalies = []
        anomalies_dir = os.path.join(self.base_dir, "anomalies")
        
        if not os.path.exists(anomalies_dir):
            return anomalies

        files = sorted(os.listdir(anomalies_dir), reverse=True)
        for file in files[:5]:  # Look at 5 most recent files
            async with aiofiles.open(os.path.join(anomalies_dir, file), 'r') as f:
                content = await f.read()
                anomalies.extend(json.loads(line) for line in content.splitlines() if line)
        
        return anomalies[-limit:]  # Return only the last 'limit' anomalies 