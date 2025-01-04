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
        
        # Enhanced normal patterns matching real logs
        self.normal_patterns = [
            "INFO org.apache.hadoop.hdfs.server.namenode.FSNamesystem: Roll Edit Log from {}",
            "INFO org.apache.hadoop.hdfs.server.namenode.FSEditLog: Number of transactions: {} Total time for transactions(ms): {} Number of transactions batched in Syncs: {}",
            "INFO org.apache.hadoop.hdfs.server.namenode.FSEditLog: Starting log segment at {}",
            '''INFO org.apache.hadoop.util.JvmPauseMonitor: Detected pause in JVM or host machine (eg GC): pause of approximately {}ms
No GCs detected''',
            "INFO org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager: Updating the current master key for generating delegation tokens",
            "INFO org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager: storing master key with keyID {}",
            "INFO org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore: Updating AMRMToken",
            "INFO org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore: Storing RMDTMasterKey."
        ]
        
        # Enhanced anomaly patterns with stack traces
        self.anomaly_patterns = [
            '''ERROR org.apache.hadoop.yarn.YarnUncaughtExceptionHandler: Thread Thread[Timer-{},5,main] threw an Exception.
java.lang.NullPointerException
    at org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager.activateNextMasterKey(RMContainerTokenSecretManager.java:146)
    at org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager$NextKeyActivator.run(RMContainerTokenSecretManager.java:167)
    at java.util.TimerThread.mainLoop(Timer.java:555)
    at java.util.TimerThread.run(Timer.java:505)''',
            
            '''INFO org.apache.hadoop.ipc.Server: Socket Reader #{} for port {}: readAndProcess from client {}:{} threw exception [java.io.IOException: Connection timed out]
java.io.IOException: Connection timed out
    at sun.nio.ch.FileDispatcherImpl.read0(Native Method)
    at sun.nio.ch.SocketDispatcher.read(SocketDispatcher.java:39)
    at sun.nio.ch.IOUtil.readIntoNativeBuffer(IOUtil.java:223)
    at sun.nio.ch.IOUtil.read(IOUtil.java:197)
    at sun.nio.ch.SocketChannelImpl.read(SocketChannelImpl.java:379)
    at org.apache.hadoop.ipc.Server.channelRead(Server.java:3270)
    at org.apache.hadoop.ipc.Server.access$2600(Server.java:137)
    at org.apache.hadoop.ipc.Server$Connection.readAndProcess(Server.java:2044)
    at org.apache.hadoop.ipc.Server$Listener.doRead(Server.java:1249)
    at org.apache.hadoop.ipc.Server$Listener$Reader.doRunLoop(Server.java:1105)
    at org.apache.hadoop.ipc.Server$Listener$Reader.run(Server.java:1076)''',
            
            "WARN org.apache.hadoop.util.JvmPauseMonitor: Detected pause in JVM or host machine (eg GC): pause of approximately {}ms\nNo GCs detected",
            "ERROR org.apache.hadoop.hdfs.server.datanode.DataNode: IOException in block {} from datanode{}: {}",
            "FATAL org.apache.hadoop.hdfs.server.datanode.DataNode: DataNode is shutting down. Reason: {}"
        ]

        self.log_storage = ElasticLogStorage()

    def generate_log(self, include_anomaly=False):
        """Generates a single log entry - keeping same interface"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        block_id = f"blk_{random.randint(1000000, 9999999)}"
        datanode = random.randint(1, 5)
        
        if include_anomaly and random.random() < 0.3:  # 30% chance of anomaly
            pattern = random.choice(self.anomaly_patterns)
            if "JvmPauseMonitor" in pattern:
                return f"{timestamp} {pattern.format(random.randint(5000, 20000))}"
            elif "Socket Reader" in pattern:
                reader_num = random.randint(1, 5)
                port = random.choice([8031, 9000])
                client_ip = f"172.18.0.{random.randint(2,4)}"
                client_port = random.randint(30000, 60000)
                return f"{timestamp} {pattern.format(reader_num, port, client_ip, client_port)}"
            elif "Timer" in pattern:
                return f"{timestamp} {pattern.format(random.randint(0, 2))}"
            elif "shutting down" in pattern:
                error_msg = random.choice([
                    "Connection refused",
                    "Disk space is too low",
                    "Network timeout",
                    "Memory allocation failed"
                ])
                return f"{timestamp} {pattern.format(error_msg)}"
            else:
                return f"{timestamp} {pattern.format(block_id, datanode, 'Connection timed out')}"
        else:
            pattern = random.choice(self.normal_patterns)
            if "transactions" in pattern:
                num_trans = random.randint(1, 10)
                trans_time = random.randint(1, 5)
                batched = random.randint(0, 2)
                return f"{timestamp} {pattern.format(num_trans, trans_time, batched)}"
            elif "Roll Edit Log" in pattern:
                return f"{timestamp} {pattern.format(f'172.18.0.{random.randint(2,4)}')}"
            elif "JvmPauseMonitor" in pattern:
                return f"{timestamp} {pattern.format(random.randint(1000, 30000))}"
            elif "storing master key" in pattern:
                return f"{timestamp} {pattern.format(random.randint(1, 10))}"
            elif "Updating" in pattern or "Storing" in pattern:
                return f"{timestamp} {pattern}"
            else:
                return f"{timestamp} {pattern.format(block_id, datanode)}"

    async def simulate_logs(self, websocket):
        """Keeps same interface but generates more realistic log sequences"""
        while True:
            try:
                num_logs = random.randint(1, 5)
                logs = []
                
                if random.random() < 0.3:
                    block_id = f"blk_{random.randint(1000000, 9999999)}"
                    datanode = random.randint(1, 5)
                    for _ in range(num_logs):
                        include_anomaly = random.random() < 0.3
                        log = self.generate_log(include_anomaly)
                        logs.append(log)
                else:
                    for _ in range(num_logs):
                        include_anomaly = random.random() < 0.3
                        log = self.generate_log(include_anomaly)
                        logs.append(log)
                
                await self.log_storage.store_raw_logs(logs)
                
                from app.api.routes import log_processor
                anomalies = await log_processor.process_logs("\n".join(logs))
                
                if anomalies:
                    await self.log_storage.store_anomalies(anomalies)
                    await websocket.send_json(anomalies)
                
                await asyncio.sleep(random.uniform(1, 3))
            except Exception as e:
                print(f"Error in log simulation: {e}")
                break 