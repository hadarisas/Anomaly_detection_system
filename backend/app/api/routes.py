from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from app.services.log_processor import LogProcessor
from app.utils.log_simulator import HDFSLogSimulator
from typing import List, Dict, Optional, Union
import asyncio
import json
from app.services.log_storage_es import ElasticLogStorage
from datetime import datetime, timedelta

router = APIRouter()
log_processor = LogProcessor()
log_simulator = HDFSLogSimulator()
log_storage = ElasticLogStorage()

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    simulation_task = None
    
    try:
        while True:
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                
                if isinstance(message, dict) and 'action' in message:
                    if message['action'] == 'start_simulation':
                        # Only start if no simulation is running
                        if simulation_task is None or simulation_task.done():
                            print("Starting simulation...")
                            simulation_task = asyncio.create_task(log_simulator.simulate_logs(websocket))
                        else:
                            print("Simulation already running")
                            
                    elif message['action'] == 'stop_simulation':
                        print("Stopping simulation...")
                        if simulation_task and not simulation_task.done():
                            simulation_task.cancel()
                            try:
                                await simulation_task
                            except asyncio.CancelledError:
                                await log_simulator.cleanup()
                            await websocket.send_json({
                                "type": "simulation_status",
                                "status": "stopped"
                            })
                
            except json.JSONDecodeError:
                print("Invalid JSON received")
            except WebSocketDisconnect:
                print("WebSocket disconnected")
                break
            except Exception as e:
                print(f"Error in websocket loop: {e}")
                break
                
    finally:
        print("Cleaning up websocket connection")
        if simulation_task and not simulation_task.done():
            simulation_task.cancel()
            try:
                await simulation_task
            except asyncio.CancelledError:
                await log_simulator.cleanup()

@router.post("/simulate-logs")
async def simulate_logs(num_logs: int = 10, include_anomalies: bool = True) -> Dict[str, List[str]]:
    logs = []
    for _ in range(num_logs):
        log = log_simulator.generate_log(include_anomalies)
        logs.append(log)
    return {"logs": logs} 


@router.get("/anomalies/recent")
async def get_recent_anomalies(time_unit: str = "5min") -> Dict:
    """Get recent anomalies with severity aggregation"""
    storage = None
    try:
        storage = ElasticLogStorage()
        await storage.initialize()
        return await storage.get_recent_anomalies(time_unit)
    except Exception as e:
        print(f"Error fetching recent anomalies: {e}")
        return {"error": str(e)}
    finally:
        if storage:
            await storage.close()

@router.get("/anomalies/history")
async def get_anomaly_history(start: Optional[str] = None, end: Optional[str] = None) -> Dict:
    """Retrieve historical anomaly data with aggregations"""
    storage = None
    try:
        storage = ElasticLogStorage()
        await storage.initialize()
        return await storage.get_anomaly_history(start_date=start, end_date=end)
    except Exception as e:
        print(f"Error fetching anomaly history: {e}")
        return {"error": str(e)}
    finally:
        if storage:
            await storage.close()

@router.get("/anomalies/logs/{limit}")
async def get_recent_anomaly_logs(limit: int = 15) -> List[Dict]:
    """Get recent anomaly logs with details"""
    return await log_storage.get_recent_logs(limit)

@router.on_event("startup")
async def startup_event():
    await log_storage.initialize()

@router.on_event("shutdown")
async def shutdown_event():
    await log_storage.close()
