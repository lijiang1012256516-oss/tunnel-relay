#!/usr/bin/env python3
"""
WebSocket Tunnel Relay Server - Render.com
Compatible with websockets 12.x, 13.x, 14.x, 15.x, 16.x
No pairing timeout - clients can connect at any time
"""

import asyncio
import argparse
import hashlib
import json
import logging
import os
import sys
import time
import traceback
from typing import Optional

try:
    import websockets
except ImportError:
    print("pip install websockets")
    exit(1)

# Log websockets version for debugging
ws_version = getattr(websockets, '__version__', 'unknown')
print(f"[STARTUP] websockets version: {ws_version}")
print(f"[STARTUP] Python version: {sys.version}")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("relay")

MSG_AUTH = "auth"
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

    @property
    def is_complete(self) -> bool:
        return self.client_ws is not None and self.server_ws is not None

class RelayServer:
    def __init__(self, secret: str):
        self.secret = secret
        self.pairs = {}
        self.lock = asyncio.Lock()

    async def handle_connection(self, websocket):
        """Handle new WebSocket connection - works with all websockets versions"""
        log.info(f"New connection from {getattr(websocket, 'remote_address', 'unknown')}")

        try:
            role, pair_id = await self._authenticate(websocket)
        except Exception as e:
            log.error(f"Auth exception: {e}\n{traceback.format_exc()}")
            try:
                await websocket.close(4001, "Auth error")
            except:
                pass
            return

        if role is None:
            log.warning("Auth failed")
            try:
                await websocket.close(4001, "Auth failed")
            except:
                pass
            return

        log.info(f"Auth OK: {role} pair={pair_id}")

        try:
            pair = await self._join_pair(websocket, role, pair_id)
        except Exception as e:
            log.error(f"Join pair exception: {e}\n{traceback.format_exc()}")
            return

        if pair is None:
            return

        # Wait for pairing (no timeout, with keepalive)
        if not pair.is_complete:
            log.info(f"{role} waiting for pair...")
            try:
                await websocket.send(json.dumps({"type": "status", "msg": "waiting"}))
                counter = 0
                while not pair.is_complete:
                    await asyncio.sleep(5)
                    counter += 1
                    # Send keepalive every 20s (counter increments every 5s)
                    if counter % 4 == 0:
                        try:
                            await websocket.send(json.dumps({"type": "status", "msg": "waiting"}))
                            log.info(f"Keepalive sent to {role} (pair={pair_id})")
                        except:
                            log.warning(f"Keepalive failed for {role}, connection lost")
                            return
            except Exception as e:
                log.error(f"Wait exception: {e}")
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
        except asyncio.TimeoutError:
            log.warning("Auth timeout - no message received")
            return None, None
        except Exception as e:
            log.warning(f"Auth recv error: {e}")
            return None, None

        log.info(f"Auth received: {raw[:200]}")

        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            log.warning(f"Invalid JSON in auth: {raw[:100]}")
            return None, None

        if msg.get("type") != MSG_AUTH:
            log.warning(f"Wrong message type: {msg.get('type')}")
            return None, None

        role = msg.get("role")
        token = msg.get("key", "")
        timestamp = msg.get("ts", 0)
        pair_id = msg.get("pair", "default")

        if role not in ("client", "server"):
            log.warning(f"Invalid role: {role}")
            return None, None

        # Detailed auth debugging
        server_time = time.time()
        time_diff = abs(server_time - timestamp)
        expected_token = make_auth_token(self.secret, timestamp)
        log.info(f"Auth check: role={role} pair={pair_id} ts={timestamp} server_ts={int(server_time)} diff={time_diff:.1f}s")
        log.info(f"Token check: client={token[:16]}... expected={expected_token[:16]}... match={token == expected_token}")

        if time_diff > TIMESTAMP_TOLERANCE:
            log.warning(f"Auth failed: timestamp diff {time_diff:.1f}s > {TIMESTAMP_TOLERANCE}s")
            return None, None

        if token != expected_token:
            log.warning(f"Auth failed: token mismatch (SECRET mismatch?)")
            return None, None

        return role, pair_id

    async def _join_pair(self, websocket, role: str, pair_id: str) -> Optional[TunnelPair]:
        async with self.lock:
            if pair_id not in self.pairs:
                self.pairs[pair_id] = TunnelPair(pair_id)
            pair = self.pairs[pair_id]
            if role == "client":
                if pair.client_ws is not None:
                    try: await pair.client_ws.close(4002, "Replaced")
                    except: pass
                pair.client_ws = websocket
            else:
                if pair.server_ws is not None:
                    try: await pair.server_ws.close(4002, "Replaced")
                    except: pass
                pair.server_ws = websocket
        return pair

    async def _relay(self, src_ws, dst_ws, direction: str):
        """Relay messages between paired clients, with keepalive"""
        async def _keepalive():
            """Send periodic pings to keep Render connection alive"""
            try:
                while True:
                    await asyncio.sleep(20)
                    try:
                        await src_ws.ping()
                    except:
                        break
                    try:
                        await dst_ws.ping()
                    except:
                        break
            except asyncio.CancelledError:
                pass

        keepalive_task = asyncio.create_task(_keepalive())
        try:
            async for raw in src_ws:
                # Skip application-level heartbeat messages
                try:
                    msg = json.loads(raw)
                    if msg.get("type") == "ping":
                        # Forward as pong to destination
                        try:
                            await dst_ws.send(json.dumps({"type": "pong", "ts": msg.get("ts", 0)}))
                        except:
                            break
                        continue
                    if msg.get("type") == "pong":
                        continue  # Skip pong, already handled
                except (json.JSONDecodeError, TypeError):
                    pass  # Not JSON, forward as-is

                try:
                    await dst_ws.send(raw)
                except Exception as e:
                    log.error(f"Relay error ({direction}): {e}")
                    break
        except websockets.ConnectionClosed as e:
            log.info(f"Closed ({direction}): code={e.code}")
        except Exception as e:
            log.error(f"Relay exception ({direction}): {e}")
        finally:
            keepalive_task.cancel()
            try: await dst_ws.close(4003, "Peer disconnected")
            except: pass
            async with self.lock:
                for pid, pair in list(self.pairs.items()):
                    if pair.client_ws is src_ws or pair.server_ws is src_ws:
                        self.pairs.pop(pid, None)
                        log.info(f"Pair cleaned: {pid}")
                        break

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
    log.info(f"SECRET configured: {args.secret[:4]}***")

    # Health check handler for Render (HEAD/GET to /health)
    async def health_check(connection, request):
        """Handle Render health checks - respond to HEAD/GET /health with HTTP 200"""
        # websockets 16.x: process_request(connection, request)
        # connection is ServerConnection, request is Request object
        path = request.path if hasattr(request, 'path') else str(request)
        method = request.method if hasattr(request, 'method') else 'GET'

        if path == '/health':
            body = b'OK'
            return (
                200,
                [("Content-Type", "text/plain"), ("Content-Length", str(len(body)))],
                body,
            )
        # Let websockets handle normal WebSocket upgrade requests
        return None

    # Build serve kwargs - compatible with all websockets versions
    serve_kwargs = {
        "handler": server.handle_connection,
        "host": "0.0.0.0",
        "port": args.port,
        "max_size": 2**20,
        "process_request": health_check,
    }

    # Add ping/close params
    serve_kwargs['ping_interval'] = HEARTBEAT_INTERVAL
    serve_kwargs['ping_timeout'] = 10
    serve_kwargs['close_timeout'] = 5

    log.info(f"serve kwargs: {list(serve_kwargs.keys())}")

    try:
        async with websockets.serve(**serve_kwargs):
            log.info("Server ready, waiting for connections...")
            await asyncio.Future()
    except Exception as e:
        log.error(f"Server failed: {e}\n{traceback.format_exc()}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Server stopped")
