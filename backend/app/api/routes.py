from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.services.log_processor import LogProcessor
from app.utils.log_simulator import HDFSLogSimulator
from typing import List, Dict
import asyncio
import json
from app.services.log_storage import LogStorage

router = APIRouter()
log_processor = LogProcessor()
log_simulator = HDFSLogSimulator()

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    simulation_task = None
    
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                if isinstance(message, dict) and 'action' in message:
                    if message['action'] == 'start_simulation':
                        print("Starting simulation...")
                        if simulation_task is None or simulation_task.done():
                            simulation_task = asyncio.create_task(log_simulator.simulate_logs(websocket))
                    elif message['action'] == 'stop_simulation':
                        print("Stopping simulation...")
                        if simulation_task and not simulation_task.done():
                            simulation_task.cancel()
                            await websocket.send_json({"status": "simulation_stopped"})
                else:
                    # Process regular log data
                    anomalies = await log_processor.process_logs(data)
                    if anomalies:
                        await websocket.send_json(anomalies)
            except json.JSONDecodeError:
                # Handle raw log data
                anomalies = await log_processor.process_logs(data)
                if anomalies:
                    await websocket.send_json(anomalies)
            except Exception as e:
                print(f"Error processing message: {e}")
                
    except WebSocketDisconnect:
        print("WebSocket disconnected")
        if simulation_task and not simulation_task.done():
            simulation_task.cancel()
    except Exception as e:
        print(f"WebSocket error: {e}")
        if simulation_task and not simulation_task.done():
            simulation_task.cancel()

@router.post("/simulate-logs")
async def simulate_logs(num_logs: int = 10, include_anomalies: bool = True) -> Dict[str, List[str]]:
    logs = []
    for _ in range(num_logs):
        log = log_simulator.generate_log(include_anomalies)
        logs.append(log)
    return {"logs": logs} 

@router.get("/logs/recent")
async def get_recent_logs(limit: int = 100):
    """Retrieve recent raw logs"""
    storage = LogStorage()
    logs = await storage.get_recent_logs(limit)
    return {"logs": logs}

@router.get("/anomalies/recent")
async def get_recent_anomalies(limit: int = 50):
    """Retrieve recent anomalies"""
    storage = LogStorage()
    anomalies = await storage.get_recent_anomalies(limit)
    return {"anomalies": anomalies} 