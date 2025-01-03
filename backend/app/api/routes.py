from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.services.log_processor import LogProcessor
from app.utils.log_simulator import HDFSLogSimulator
from typing import List, Dict, Optional
import asyncio
import json
from app.services.log_storage_es import ElasticLogStorage
from datetime import datetime

router = APIRouter()
log_processor = LogProcessor()
log_simulator = HDFSLogSimulator()

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

@router.post("/simulate-logs")
async def simulate_logs(num_logs: int = 10, include_anomalies: bool = True) -> Dict[str, List[str]]:
    logs = []
    for _ in range(num_logs):
        log = log_simulator.generate_log(include_anomalies)
        logs.append(log)
    return {"logs": logs} 


@router.get("/anomalies/history")
async def get_anomaly_history(
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    """Retrieve historical anomaly data with aggregations"""
    storage = None
    try:
        storage = ElasticLogStorage()
        await storage.initialize()

        # Calculate appropriate interval based on time difference
        if start and end:
            start_date = start
            end_date = end
            try:
                # Handle timezone-aware datetime strings
                start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                hours_diff = (end_dt - start_dt).total_seconds() / 3600
                
                # Choose appropriate interval
                if hours_diff <= 6:
                    interval = "5m"
                elif hours_diff <= 24:
                    interval = "15m"
                elif hours_diff <= 168:  # 7 days
                    interval = "1h"
                else:
                    interval = "6h"
            except ValueError as e:
                print(f"Error parsing dates: {e}")
                # Default to last 24 hours if date parsing fails
                end_date = "now"
                start_date = "now-24h"
                interval = "1h"
        else:
            # Default to last 24 hours
            end_date = "now"
            start_date = "now-24h"
            interval = "1h"

        history = await storage.get_anomaly_history(
            start_date=start_date,
            end_date=end_date,
            interval=interval
        )
        return history
    except Exception as e:
        print(f"Error fetching anomaly history: {e}")
        raise
    finally:
        if storage:
            await storage.close()
