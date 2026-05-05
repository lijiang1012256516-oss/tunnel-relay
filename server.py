#!/usr/bin/env python3
"""
WebSocket Tunnel Relay Server - Render.com
No pairing timeout - clients can connect at any time
"""

import asyncio
import argparse
import hashlib
import json
import logging
import os
import time
from typing import Optional

try:
    import websockets
    try:
        from websockets.server import serve
    except ImportError:
        from websockets.legacy.server import serve
except ImportError:
    print("pip install websockets")
    exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("relay")

MSG_AUTH = "auth"
MSG_CONNECT = "connect"
MSG_CONNECT_OK = "connect_ok"
MSG_CONNECT_FAIL = "connect_fail"
MSG_DATA = "data"
MSG_CLOSE = "close"
MSG_PING = "ping"
MSG_PONG = "pong"

AUTH_TIMEOUT = 10
TIMESTAMP_TOLERANCE = 60
HEARTBEAT_INTERVAL = 30

def make_auth_token(secret: str, timestamp: int) -> str:
    return hashlib.sha256(f"{secret}{timestamp}".encode()).hexdigest()

def verify_auth(secret: str, token: str, timestamp: int) -> bool:
    if abs(time.time() - timestamp) > TIMESTAMP_TOLERANCE:
        return False
    return token == make_auth_token(secret, timestamp)

class TunnelPair:
    def __init__(self, pair_id: str):
        self.pair_id = pair_id
        self.client_ws = None
        self.server_ws = None
        self.created_at = time.time()

    @property
    def is_complete(self) -> bool:
        return self.client_ws is not None and self.server_ws is not None

class RelayServer:
    def __init__(self, secret: str):
        self.secret = secret
        self.pairs = {}
        self.lock = asyncio.Lock()

    async def handle_connection(self, websocket):
        remote_addr = websocket.remote_address
        log.info(f"New connection from {remote_addr}")

        role, pair_id = await self._authenticate(websocket)
        if role is None:
            log.warning(f"Auth failed: {remote_addr}")
            await websocket.close(4001, "Auth failed")
            return

        log.info(f"Auth OK: {role} pair={pair_id} @ {remote_addr}")

        pair = await self._join_pair(websocket, role, pair_id)
        if pair is None:
            return

        if not pair.is_complete:
            log.info(f"{role} waiting for pair (pair={pair_id})...")
            try:
                await websocket.send(json.dumps({"type": "status", "msg": "waiting"}))
                counter = 0
                while not pair.is_complete:
                    await asyncio.sleep(2)
                    counter += 1
                    if counter % 15 == 0:
                        try:
                            await websocket.send(json.dumps({"type": "status", "msg": "waiting"}))
                        except:
                            return
            except:
                return

        if not pair.is_complete:
            return

        log.info(f"Pair complete! pair={pair_id}")

        for ws in [pair.client_ws, pair.server_ws]:
            try:
                await ws.send(json.dumps({"type": "status", "msg": "paired"}))
            except:
                pass

        if role == "client":
            await self._relay(pair.client_ws, pair.server_ws, "Mac->Win")
        else:
            await self._relay(pair.server_ws, pair.client_ws, "Win->Mac")

    async def _authenticate(self, websocket) -> tuple:
        try:
            raw = await asyncio.wait_for(websocket.recv(), timeout=AUTH_TIMEOUT)
        except:
            return None, None
        try:
            msg = json.loads(raw)
        except:
            return None, None
        if msg.get("type") != MSG_AUTH:
            return None, None
        role = msg.get("role")
        token = msg.get("key", "")
        timestamp = msg.get("ts", 0)
        pair_id = msg.get("pair", "default")
        if role not in ("client", "server"):
            return None, None
        if not verify_auth(self.secret, token, timestamp):
            return None, None
        return role, pair_id

    async def _join_pair(self, websocket, role: str, pair_id: str) -> Optional[TunnelPair]:
        async with self.lock:
            if pair_id not in self.pairs:
                self.pairs[pair_id] = TunnelPair(pair_id)
            pair = self.pairs[pair_id]
            if role == "client":
                if pair.client_ws is not None:
                    try:
                        await pair.client_ws.close(4002, "Replaced")
                    except:
                        pass
                pair.client_ws = websocket
            else:
                if pair.server_ws is not None:
                    try:
                        await pair.server_ws.close(4002, "Replaced")
                    except:
                        pass
                pair.server_ws = websocket
        return pair

    async def _relay(self, src_ws, dst_ws, direction: str):
        try:
            async for raw in src_ws:
                try:
                    await dst_ws.send(raw)
                except Exception as e:
                    log.error(f"Relay error ({direction}): {e}")
                    break
        except websockets.ConnectionClosed as e:
            log.info(f"Connection closed ({direction}): code={e.code}")
        except Exception as e:
            log.error(f"Relay exception ({direction}): {e}")
        finally:
            try:
                await dst_ws.close(4003, "Peer disconnected")
            except:
                pass
            async with self.lock:
                for pid, pair in list(self.pairs.items()):
                    if pair.client_ws is src_ws or pair.server_ws is src_ws:
                        self.pairs.pop(pid, None)
                        log.info(f"Pair cleaned: {pid}")
                        break

async def health_handler(path, request_headers):
    if path == "/health":
        return (200, [], b"OK")
    return None

async def main():
    port = int(os.environ.get("PORT", "8765"))
    secret = os.environ.get("SECRET", "")
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=port)
    parser.add_argument("--secret", default=secret)
    args = parser.parse_args()
    if not args.secret:
        print("Error: SECRET env var or --secret required")
        exit(1)
    server = RelayServer(secret=args.secret)
    log.info(f"Relay server starting: 0.0.0.0:{args.port}")
    log.info(f"Secret: {args.secret[:4]}{'*' * (len(args.secret) - 4)}")
    async with serve(
        server.handle_connection,
        "0.0.0.0",
        args.port,
        ping_interval=HEARTBEAT_INTERVAL,
        ping_timeout=10,
        close_timeout=5,
        max_size=2**20,
        process_request=health_handler,
    ):
        log.info("Server ready, waiting for connections...")
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Server stopped")
