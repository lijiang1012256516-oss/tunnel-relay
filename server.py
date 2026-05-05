#!/usr/bin/env python3
"""
WebSocket 隧道中继服务器 — Render.com 部署版

部署位置: Render.com 免费 Web Service
职责: 接受 Mac 和 Windows 的 WebSocket 连接，双向转发数据

部署步骤:
1. 创建 GitHub 仓库，包含 server.py + requirements.txt
2. 在 Render.com 创建 Web Service，连接该仓库
3. 设置环境变量 SECRET=你的共享密钥
4. 部署完成后获得 https://xxx.onrender.com

本地测试:
  python server.py --port 8765 --secret test123
"""

import asyncio
import argparse
import hashlib
import json
import logging
import os
import time
from typing import Optional
from collections import defaultdict

try:
    import websockets
    from websockets.server import serve
except ImportError:
    print("请安装 websockets: pip install websockets")
    exit(1)

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("relay")

# ─── 协议常量 ───────────────────────────────────────────────

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
MAX_QUEUE_SIZE = 1000

# ─── 认证 ───────────────────────────────────────────────────

def make_auth_token(secret: str, timestamp: int) -> str:
    return hashlib.sha256(f"{secret}{timestamp}".encode()).hexdigest()

def verify_auth(secret: str, token: str, timestamp: int) -> bool:
    if abs(time.time() - timestamp) > TIMESTAMP_TOLERANCE:
        return False
    return token == make_auth_token(secret, timestamp)

# ─── 隧道配对管理 ───────────────────────────────────────────

class TunnelPair:
    """一对配对的 WebSocket 连接 (Mac + Windows)"""
    
    def __init__(self, pair_id: str):
        self.pair_id = pair_id
        self.client_ws: Optional[websockets.WebSocketServerProtocol] = None   # Mac
        self.server_ws: Optional[websockets.WebSocketServerProtocol] = None   # Windows
        self.created_at = time.time()
    
    @property
    def is_complete(self) -> bool:
        return self.client_ws is not None and self.server_ws is not None


class RelayServer:
    """中继服务器核心 — 支持多隧道配对"""
    
    def __init__(self, secret: str):
        self.secret = secret
        # 默认隧道 (无 pair_id 时使用)
        self.default_pair = TunnelPair("default")
        # 按 pair_id 索引的隧道
        self.pairs: dict[str, TunnelPair] = {}
        self.lock = asyncio.Lock()
    
    async def handle_connection(self, websocket):
        """处理新的 WebSocket 连接"""
        remote_addr = websocket.remote_address
        log.info(f"新连接来自 {remote_addr}")
        
        # 1. 认证
        role, pair_id = await self._authenticate(websocket)
        if role is None:
            log.warning(f"认证失败，关闭连接 {remote_addr}")
            await websocket.close(4001, "认证失败")
            return
        
        log.info(f"认证成功: {role} pair={pair_id} @ {remote_addr}")
        
        # 2. 加入隧道配对
        pair = await self._join_pair(websocket, role, pair_id)
        if pair is None:
            return
        
        # 3. 等待配对完成
        if not pair.is_complete:
            log.info(f"{role} 等待配对 (pair={pair_id})...")
            try:
                await websocket.send(json.dumps({"type": "status", "msg": "waiting"}))
                for _ in range(120):  # 最多等 60 秒
                    if pair.is_complete:
                        break
                    await asyncio.sleep(0.5)
            except:
                return
        
        if not pair.is_complete:
            await websocket.close(4004, "配对超时")
            return
        
        log.info(f"隧道配对完成! pair={pair_id} Mac={pair.client_ws.remote_address}, Windows={pair.server_ws.remote_address}")
        
        # 4. 通知双方配对成功
        for ws in [pair.client_ws, pair.server_ws]:
            try:
                await ws.send(json.dumps({"type": "status", "msg": "paired"}))
            except:
                pass
        
        # 5. 启动双向转发
        if role == "client":
            await self._relay(pair.client_ws, pair.server_ws, "Mac→Win")
        else:
            await self._relay(pair.server_ws, pair.client_ws, "Win→Mac")
    
    async def _authenticate(self, websocket) -> tuple:
        """等待并验证认证消息，返回 (role, pair_id) 或 (None, None)"""
        try:
            raw = await asyncio.wait_for(websocket.recv(), timeout=AUTH_TIMEOUT)
        except asyncio.TimeoutError:
            return None, None
        except Exception:
            return None, None
        
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
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
        """将连接加入隧道配对"""
        async with self.lock:
            if pair_id not in self.pairs:
                self.pairs[pair_id] = TunnelPair(pair_id)
            
            pair = self.pairs[pair_id]
            
            if role == "client":
                if pair.client_ws is not None:
                    log.warning(f"已有 Mac 连接 (pair={pair_id})，替换")
                    try:
                        await pair.client_ws.close(4002, "被新连接替换")
                    except:
                        pass
                pair.client_ws = websocket
            else:
                if pair.server_ws is not None:
                    log.warning(f"已有 Windows 连接 (pair={pair_id})，替换")
                    try:
                        await pair.server_ws.close(4002, "被新连接替换")
                    except:
                        pass
                pair.server_ws = websocket
        
        return pair
    
    async def _relay(self, src_ws, dst_ws, direction: str):
        """从 src 转发消息到 dst"""
        try:
            async for raw in src_ws:
                try:
                    await dst_ws.send(raw)
                except Exception as e:
                    log.error(f"转发失败 ({direction}): {e}")
                    break
        except websockets.ConnectionClosed as e:
            log.info(f"连接关闭 ({direction}): code={e.code} reason={e.reason}")
        except Exception as e:
            log.error(f"转发异常 ({direction}): {e}")
        finally:
            try:
                await dst_ws.close(4003, "对端断开")
            except:
                pass
            # 清理配对
            async with self.lock:
                # 找到并清理包含断开连接的配对
                for pid, pair in list(self.pairs.items()):
                    if pair.client_ws is src_ws or pair.server_ws is src_ws:
                        self.pairs.pop(pid, None)
                        log.info(f"隧道配对已清理: pair={pid}")
                        break


# ─── 健康检查 HTTP 处理 ────────────────────────────────────

async def health_handler(path, request_headers):
    """HTTP 健康检查 (Render 需要此功能)"""
    # 只对 /health 路径返回 HTTP 响应
    # 其他所有路径 (包括 /) 都交给 WebSocket 处理
    if path == "/health":
        return (200, [], b"OK")
    # 交给 WebSocket 处理
    return None


# ─── 主入口 ─────────────────────────────────────────────────

async def main():
    # 从环境变量读取配置 (Render 部署方式)
    port = int(os.environ.get("PORT", "8765"))
    secret = os.environ.get("SECRET", "")
    
    # 也支持命令行参数 (本地测试)
    parser = argparse.ArgumentParser(description="WebSocket 隧道中继服务器")
    parser.add_argument("--port", type=int, default=port, help="监听端口")
    parser.add_argument("--secret", default=secret, help="共享密钥")
    args = parser.parse_args()
    
    if not args.secret:
        print("错误: 必须设置 SECRET 环境变量或 --secret 参数")
        exit(1)
    
    server = RelayServer(secret=args.secret)
    
    log.info(f"中继服务器启动: 0.0.0.0:{args.port}")
    log.info(f"共享密钥: {args.secret[:4]}{'*' * (len(args.secret) - 4)}")
    
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
        log.info("服务器已就绪，等待连接...")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("服务器已停止")
