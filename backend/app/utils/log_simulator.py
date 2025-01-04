import random
from datetime import datetime
import asyncio
import json
from app.services.log_storage_es import ElasticLogStorage
from pathlib import Path
from app.services.log_processor import LogProcessor

class HDFSLogSimulator:
    def __init__(self):
        self.block_ids = list(range(1000, 2000))
        self.datanodes = [f"datanode{i}" for i in range(1, 6)]
        self.clients = [f"client-{i}" for i in range(1, 4)]
        
        # normal patterns matching real logs
        self.normal_patterns = [
            "INFO org.apache.hadoop.hdfs.server.namenode.FSNamesystem: Roll Edit Log from {}",
            "INFO org.apache.hadoop.hdfs.server.namenode.FSEditLog: Number of transactions: {} Total time for transactions(ms): {} Number of transactions batched in Syncs: {}",
            "INFO org.apache.hadoop.hdfs.server.namenode.FSEditLog: Starting log segment at {}",
            "INFO org.apache.hadoop.util.JvmPauseMonitor: Detected pause in JVM or host machine (eg GC): pause of approximately {}ms\nNo GCs detected",
            "INFO org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager: Updating the current master key for generating delegation tokens",
            "INFO org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager: storing master key with keyID {}",
            "INFO org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore: Updating AMRMToken",
            "INFO org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore: Storing RMDTMasterKey.",
            "INFO org.apache.hadoop.hdfs.server.namenode.FSNamesystem: Completed loading FSImage in {} milliseconds",
            "INFO org.apache.hadoop.hdfs.server.namenode.FSNamesystem: Starting checkpoint for transaction ID {}",
            "INFO org.apache.hadoop.hdfs.server.namenode.ha.ActiveStandbyElector: Successfully claimed leadership",
            "INFO org.apache.hadoop.hdfs.server.datanode.DataNode: Block pool {} (Datanode Uuid {}) service has been successfully registered with NN",
            "INFO org.apache.hadoop.hdfs.server.blockmanagement.BlockManager: Total load = {}, number of live nodes = {}"
        ]
        
        # anomaly patterns with stack traces
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
            "FATAL org.apache.hadoop.hdfs.server.datanode.DataNode: DataNode is shutting down. Reason: {}",
            "WARN org.apache.hadoop.hdfs.server.namenode.FSNamesystem: Disk usage {} exceeds threshold {}",
            "ERROR org.apache.hadoop.hdfs.server.namenode.ha.ActiveStandbyElector: Lost leadership",
            "FATAL org.apache.hadoop.hdfs.server.namenode.NameNode: Failed to start active state service",
            "ERROR org.apache.hadoop.hdfs.server.blockmanagement.BlockManager: Block {} is corrupted on {} datanodes",
            "WARN org.apache.hadoop.security.UserGroupInformation: Login failed for user {} due to {}"
        ]

        self.log_storage = None
        self.log_processor = LogProcessor()

        # log directory setup
        self.log_dir = Path("logs/raw")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.current_log_file = None
        self.init_log_file()

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

    def _generate_jvm_pause(self):
        """Generate JVM pause log with clear severity levels"""
        duration = random.randint(5000, 30000)
        level = "WARN" if duration > 15000 else "INFO"
        return f"{self._get_timestamp()} {level} org.apache.hadoop.util.JvmPauseMonitor: " \
               f"Detected pause in JVM or host machine (eg GC): pause of approximately {duration}ms"

    def _generate_connection_error(self):
        """Generate connection error log with specific timeout indication"""
        return f"{self._get_timestamp()} ERROR org.apache.hadoop.hdfs.server.datanode.DataNode: " \
               f"IOException in block blk_{random.randint(1000, 9999999)} from datanode{random.randint(1,5)}: Connection timed out"

    def _generate_thread_error(self):
        """Generate thread error log with specific exception type"""
        exceptions = [
            "UncaughtException",
            "NullPointerException",
            "RuntimeException"
        ]
        return f"{self._get_timestamp()} ERROR org.apache.hadoop.yarn.YarnUncaughtExceptionHandler: " \
               f"Thread Thread[Timer-{random.randint(1,3)},5,main] threw an {random.choice(exceptions)}"

    def _generate_datanode_error(self):
        """Generate DataNode error log with specific error types"""
        errors = [
            f"ERROR org.apache.hadoop.hdfs.server.datanode.DataNode: Block blk_{random.randint(1000, 9999)} is corrupted",
            f"ERROR org.apache.hadoop.hdfs.server.datanode.DataNode: DataNode shutdown due to Block pool BP-{random.randint(1000, 9999)} service failure",
            f"FATAL org.apache.hadoop.hdfs.server.datanode.DataNode: DataNode is shutting down due to disk failure",
            f"ERROR org.apache.hadoop.hdfs.server.datanode.DataNode: Failed to complete block protocol BP-{random.randint(1000, 9999)}"
        ]
        return f"{self._get_timestamp()} {random.choice(errors)}"

    def _generate_security_error(self):
        """Generate security error log with specific security issues"""
        security_errors = [
            f"ERROR org.apache.hadoop.security.SecurityManager: Security token {random.randint(1000, 9999)} has expired",
            f"ERROR org.apache.hadoop.security.SecurityManager: Token validation failed for user-{random.randint(1, 100)}",
            f"ERROR org.apache.hadoop.security.SecurityManager: Authentication failed for service token"
        ]
        return f"{self._get_timestamp()} {random.choice(security_errors)}"

    def _generate_namenode_error(self):
        """Generate NameNode specific errors and warnings"""
        errors = [
            f"ERROR org.apache.hadoop.hdfs.server.namenode.ha.ActiveStandbyElector: Lost leadership due to session expiry",
            f"FATAL org.apache.hadoop.hdfs.server.namenode.NameNode: Failed to start active state service - Already running on node{random.randint(1,5)}",
            f"WARN org.apache.hadoop.hdfs.server.namenode.FSNamesystem: Disk usage {random.randint(85, 99)}% exceeds threshold 80%",
            f"ERROR org.apache.hadoop.hdfs.server.namenode.FSNamesystem: Failed to complete checkpoint for transaction ID {random.randint(10000, 99999)}"
        ]
        return f"{self._get_timestamp()} {random.choice(errors)}"

    def _generate_block_error(self):
        """Generate block management related errors"""
        block_id = f"blk_{random.randint(1000, 9999)}"
        errors = [
            f"ERROR org.apache.hadoop.hdfs.server.blockmanagement.BlockManager: Block {block_id} is corrupted on {random.randint(1, 3)} datanodes",
            f"WARN org.apache.hadoop.hdfs.server.blockmanagement.BlockManager: Total load = {random.randint(80, 95)}%, number of live nodes = {random.randint(1, 4)}",
            f"ERROR org.apache.hadoop.hdfs.server.blockmanagement.BlockManager: Unable to place replica block {block_id} on any node"
        ]
        return f"{self._get_timestamp()} {random.choice(errors)}"

    def _generate_authentication_error(self):
        """Generate authentication and authorization errors"""
        user = f"user_{random.randint(1, 100)}"
        reasons = [
            "Invalid credentials",
            "Token expired",
            "Permission denied",
            "User not authorized for this operation",
            "Kerberos authentication failed"
        ]
        return f"{self._get_timestamp()} WARN org.apache.hadoop.security.UserGroupInformation: Login failed for user {user} due to {random.choice(reasons)}"

    def generate_log(self, include_anomaly=False):
        """Keeps same interface but with enhanced anomaly generation"""
        if include_anomaly and random.random() < 0.3:  # 30% chance of anomaly
            anomaly_type = random.choices([
                self._generate_jvm_pause,
                self._generate_connection_error,
                self._generate_thread_error,
                self._generate_datanode_error,
                self._generate_security_error,
                self._generate_namenode_error,
                self._generate_block_error,
                self._generate_authentication_error
            ], weights=[0.2, 0.15, 0.15, 0.1, 0.1, 0.1, 0.1, 0.1])[0]  # weights for anomaly types
            return anomaly_type()
        else:
            pattern = random.choice(self.normal_patterns)
            # normal log generation logic
            if "transactions" in pattern:
                num_trans = random.randint(1, 10)
                trans_time = random.randint(1, 5)
                batched = random.randint(0, 2)
                return f"{self._get_timestamp()} {pattern.format(num_trans, trans_time, batched)}"
            elif "Roll Edit Log" in pattern:
                return f"{self._get_timestamp()} {pattern.format(f'172.18.0.{random.randint(2,4)}')}"
            elif "JvmPauseMonitor" in pattern:
                return f"{self._get_timestamp()} {pattern.format(random.randint(1000, 3000))}"
            elif "storing master key" in pattern:
                return f"{self._get_timestamp()} {pattern.format(random.randint(1, 10))}"
            else:
                return f"{self._get_timestamp()} {pattern}"

    def _get_timestamp(self):
        """Helper method to generate timestamp"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    async def initialize(self):
        """Initialize connections and resources"""
        self.log_storage = ElasticLogStorage()
        await self.log_storage.initialize()

    async def cleanup(self):
        """Cleanup resources and connections"""
        if self.log_storage:
            await self.log_storage.close()
            self.log_storage = None

    async def simulate_logs(self, websocket):
        """Simulate and store logs"""
        try:
            await self.initialize()
            while True:
                try:
                    num_logs = random.randint(1, 5)
                    logs = []
                    
                    for _ in range(num_logs):
                        include_anomaly = random.random() < 0.3
                        log = self.generate_log(include_anomaly)
                        logs.append(log)
                    
                    self.write_to_log_file(logs)
                    
                    await self.log_storage.store_raw_logs(logs)
                    
                    anomalies = await self.log_processor.process_logs("\n".join(logs))
                    
                    if anomalies:
                        await self.log_storage.store_anomalies(anomalies)
                        await websocket.send_json(anomalies)
                    
                    await asyncio.sleep(random.uniform(1, 3))
                except Exception as e:
                    print(f"Error in log simulation: {e}")
                    break
        finally:
            await self.cleanup() 