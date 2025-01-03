import random
from datetime import datetime
import asyncio
import json
from app.services.log_storage_es import ElasticLogStorage

class HDFSLogSimulator:
    def __init__(self):
        self.block_ids = list(range(1000, 2000))
        self.datanodes = [f"datanode{i}" for i in range(1, 6)]
        self.clients = [f"client-{i}" for i in range(1, 4)]
        
        # Normal log patterns
        self.normal_patterns = [
            "INFO dfs.DataNode$DataXceiver: Receiving block {} src: /{}",
            "INFO dfs.DataNode$PacketResponder: Received block {} of size {} from {}",
            "INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: block {} is added to {} size {}",
            "INFO dfs.DataNode$DataXceiver: block {} received from {}"
        ]
        
        # Anomaly patterns
        self.anomaly_patterns = [
            "ERROR dfs.DataNode$DataXceiver: IOException in block {} from {} {}",
            "WARN dfs.DataNode: Slow BlockReceiver write data to disk cost:{} for block {}",
            "ERROR dfs.FSNamesystem: BLOCK* NameSystem.delete: block {} is corrupted",
            "ERROR dfs.DataNode$DataXceiver: writeBlock {} received exception {}",
            "FATAL dfs.DataNode: DataNode is shutting down: {}",
        ]

        self.log_storage = ElasticLogStorage()

    def generate_log(self, include_anomaly=False):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        block_id = f"blk_{random.randint(1000000, 9999999)}"
        datanode = random.choice(self.datanodes)
        client = random.choice(self.clients)
        size = f"{random.randint(1, 1024)}MB"
        
        if include_anomaly and random.random() < 0.3:  # 30% chance of anomaly
            pattern = random.choice(self.anomaly_patterns)
            error_msg = random.choice([
                "Connection refused",
                "Disk I/O error",
                "Network timeout",
                "Checksum failed"
            ])
            log_message = pattern.format(block_id, datanode, error_msg)
        else:
            pattern = random.choice(self.normal_patterns)
            log_message = pattern.format(block_id, datanode, size, client)
            
        return f"{timestamp} {log_message}"

    async def simulate_logs(self, websocket):
        while True:
            try:
                # Generate between 1-5 logs
                num_logs = random.randint(1, 5)
                logs = []
                
                for _ in range(num_logs):
                    include_anomaly = random.random() < 0.3
                    log = self.generate_log(include_anomaly)
                    logs.append(log)
                
                # Store raw logs
                await self.log_storage.store_raw_logs(logs)
                
                # Process logs and detect anomalies
                from app.api.routes import log_processor
                anomalies = await log_processor.process_logs("\n".join(logs))
                
                if anomalies:
                    # Store and send anomalies
                    await self.log_storage.store_anomalies(anomalies)
                    await websocket.send_json(anomalies)
                
                await asyncio.sleep(random.uniform(1, 3))
            except Exception as e:
                print(f"Error in log simulation: {e}")
                break 