"""WebSocket endpoint for live pipeline state, GPU, and control updates.

Routes:
    WS /api/ws - live update stream
"""

import asyncio
import os

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from paths import PIPELINE_STATE_DB
from server.helpers import _get_pipeline_state, file_exists, get_pause_state
from server.routers.admin import _query_gpu

router = APIRouter()


class ConnectionManager:
    """Manage WebSocket connections for live pipeline updates."""

    def __init__(self) -> None:
        self.connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket) -> None:
        """Accept and track a new WebSocket connection."""
        await ws.accept()
        self.connections.append(ws)

    def disconnect(self, ws: WebSocket) -> None:
        """Remove a WebSocket connection from tracking."""
        if ws in self.connections:
            self.connections.remove(ws)

    async def broadcast(self, data: dict) -> None:
        """Send data to all connected clients, pruning dead connections."""
        dead: list[WebSocket] = []
        for ws in self.connections:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


ws_manager = ConnectionManager()
_ws_state_mtime: float = 0


@router.websocket("/api/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    """WebSocket endpoint that pushes live pipeline, GPU, and control updates."""
    global _ws_state_mtime
    await ws_manager.connect(ws)
    try:
        # Send initial state
        state_data = _get_pipeline_state()
        if state_data:
            await ws.send_json({"type": "pipeline", "data": state_data})

        gpu_data = _query_gpu()
        await ws.send_json({"type": "gpu", "data": gpu_data})

        control_data = {
            "pause_state": get_pause_state(),
            "has_skip": file_exists("skip.json"),
            "has_priority": file_exists("priority.json"),
            "has_gentle": file_exists("gentle.json"),
            "has_reencode": file_exists("reencode.json"),
        }
        await ws.send_json({"type": "control", "data": control_data})

        # Poll loop -- push updates when state changes
        gpu_tick = 0
        while True:
            try:
                await asyncio.wait_for(ws.receive_text(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break

            # Pipeline state -- check DB mtime
            try:
                db_path = str(PIPELINE_STATE_DB)
                wal_path = db_path + "-wal"
                if os.path.exists(wal_path):
                    mtime = os.path.getmtime(wal_path)
                elif os.path.exists(db_path):
                    mtime = os.path.getmtime(db_path)
                else:
                    mtime = 0
            except OSError:
                mtime = 0
            if mtime != _ws_state_mtime:
                _ws_state_mtime = mtime
                data = _get_pipeline_state()
                if data:
                    await ws.send_json({"type": "pipeline", "data": data})

            # GPU stats every 5 ticks (~5 seconds)
            gpu_tick += 1
            if gpu_tick >= 5:
                gpu_tick = 0
                await ws.send_json({"type": "gpu", "data": _query_gpu()})

            # Control status every tick
            new_control = {
                "pause_state": get_pause_state(),
                "has_skip": file_exists("skip.json"),
                "has_priority": file_exists("priority.json"),
                "has_gentle": file_exists("gentle.json"),
                "has_reencode": file_exists("reencode.json"),
            }
            if new_control != control_data:
                control_data = new_control
                await ws.send_json({"type": "control", "data": control_data})

    except WebSocketDisconnect:
        pass
    finally:
        ws_manager.disconnect(ws)
