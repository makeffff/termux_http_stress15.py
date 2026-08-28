import argparse
import asyncio
import csv
import gc
import os
import queue as queue_mod
import random
import signal
import socket
import ssl
import struct
import sys
import time
import json
import multiprocessing
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Any
from urllib.parse import urlparse

# ===========================================================================
# 可选依赖检测
# ===========================================================================
HAS_AIOHTTP = False
try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    pass

HAS_H2 = False
try:
    import h2.connection
    import h2.config
    import h2.events
    import h2.settings
    import h2.exceptions
    HAS_H2 = True
except ImportError:
    pass

HAS_AIOQUIC = False
try:
    from aioquic.asyncio import connect as quic_connect
    from aioquic.quic.configuration import QuicConfiguration
    from aioquic.h3.connection import H3Connection
    from aioquic.h3.events import H3Event, HeadersReceived, DataReceived, StreamEnded
    HAS_AIOQUIC = True
except ImportError:
    pass

HAS_AIODNS = False
try:
    import aiodns
    HAS_AIODNS = True
except ImportError:
    pass

HAS_UVLOOP = False
try:
    import uvloop
    HAS_UVLOOP = True
except ImportError:
    pass

HAS_YAML = False
try:
    import yaml
    HAS_YAML = True
except ImportError:
    pass

HAS_WEBSOCKETS = False
try:
    import websockets
    HAS_WEBSOCKETS = True
except ImportError:
    pass

try:
    import orjson
    def json_dumps(obj) -> bytes:
        return orjson.dumps(obj)
except ImportError:
    def json_dumps(obj) -> bytes:
        return json.dumps(obj).encode()

# [H3] Python 3.11+ 的 C 实现超时上下文
_TIMEOUT_CTX = getattr(asyncio, "timeout", None)

VERBOSE = True
def _log(msg):
    if VERBOSE:
        print(msg, flush=True)


# ===========================================================================
# 常量池
# ===========================================================================
VERSION = "22.0"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "curl/8.4.0", "python-requests/2.31.0", "Go-http-client/1.1",
]
ACCEPT_LANGUAGES = ["en-US,en;q=0.9", "en-GB,en;q=0.8", "zh-CN,zh;q=0.9,en;q=0.8"]
CACHE_CONTROL_VALUES = ["no-cache", "no-cache, no-store", "max-age=0"]
CACHE_BUSTER_KEYS = ["_", "cb", "cachebust", "nocache", "t", "ts", "rand", "r", "v", "x"]

MAX_H1_HEADER = 1024 * 1024
MAX_H1_CHUNK_LINE = 64 * 1024
H2_SEND_CHUNK = 16384
H2_INIT_WINDOW = 1 << 20
H2_MAX_FRAME = 1 << 20
H2_WINDOW_ACK_THRESHOLD = 524288
MAX_ERROR_KEYS = 256


# ===========================================================================
# 授权声明
# ===========================================================================
AUTHORIZATION_BANNER = """
======================================================================
                  AUTHORIZATION REQUIRED
  This tool is for AUTHORIZED stress testing ONLY.
  Unauthorized use may violate laws (CFAA, 刑法285/286, etc).
  Use --i-have-authorization to confirm you have permission.
======================================================================
"""


# ===========================================================================
# 工具函数
# ===========================================================================
def random_cache_buster():
    key = random.choice(CACHE_BUSTER_KEYS)
    val = random.randint(100000000000000, 999999999999999)
    return f"{key}={val}"

def random_query_string(existing):
    parts = [random_cache_buster() for _ in range(random.randint(1, 3))]
    parts.append(f"tok={random.getrandbits(64):016x}")
    suffix = "&".join(parts)
    return existing + "&" + suffix if existing else suffix

def random_header_set():
    return {
        "cache-control": random.choice(CACHE_CONTROL_VALUES),
        "pragma": "no-cache",
        "accept-language": random.choice(ACCEPT_LANGUAGES),
        "user-agent": random.choice(USER_AGENTS),
        "accept": "*/*",
    }

def parse_alt_svc(header_value: str) -> list:
    results = []
    if not header_value: return results
    for entry in header_value.split(','):
        entry = entry.strip()
        if '=' not in entry: continue
        proto_part, rest = entry.split('=', 1)
        proto = proto_part.strip().lower()
        if not proto.startswith('h3'): continue
        rest = rest.strip().strip('"')
        if rest.startswith(':'):
            try: results.append({'proto': proto, 'host': '', 'port': int(rest[1:])})
            except ValueError: pass
        elif ':' in rest:
            host, port_s = rest.rsplit(':', 1)
            try: results.append({'proto': proto, 'host': host, 'port': int(port_s)})
            except ValueError: pass
    return results

def build_ep_group_map(groups) -> Dict[int, Any]:
    m = {}
    for g in groups:
        for ep in g.endpoints: m[id(ep)] = g
    return m

def is_conn_level_error(exc: BaseException) -> bool:
    if isinstance(exc, asyncio.TimeoutError): return False
    if isinstance(exc, (ConnectionError, ssl.SSLError, OSError)): return True
    name = type(exc).__name__
    return name in ("ProtocolError", "ConnectionTerminated", "GoawayError", "HandshakeError", "QuicConnectionError", "StreamClosedError")

async def supervised(name: str, factory, stop_event: asyncio.Event):
    backoff = 1.0
    while not stop_event.is_set():
        started = time.monotonic()
        try:
            await factory()
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            ran = time.monotonic() - started
            if ran > 60.0: backoff = 1.0
            _log(f"[WARN] worker {name} crashed: {type(e).__name__}: {e}; restarting in {backoff:.0f}s")
            try: await asyncio.sleep(backoff)
            except asyncio.CancelledError: raise
            backoff = min(backoff * 2.0, 30.0)

def render_template(template: str, data: dict) -> str:
    if not template: return template
    for k, v in data.items():
        template = template.replace(f"${{{k}}}", str(v))
    return template


# ===========================================================================
# [H3] 超时执行助手: 3.11+ 免 Task 分配
# ===========================================================================
async def _run_timed(coro, timeout: float):
    if _TIMEOUT_CTX is not None:
        async with _TIMEOUT_CTX(timeout):
            return await coro
    return await asyncio.wait_for(coro, timeout=timeout)


# ===========================================================================
# 端点健康冷却器
# ===========================================================================
class EndpointHealth:
    def __init__(self, cooldown: float = 2.0):
        self._cooldown = cooldown
        self._failed_until: Dict[str, float] = {}

    def ready(self, key: str) -> bool:
        return time.monotonic() >= self._failed_until.get(key, 0.0)

    def mark_fail(self, key: str):
        self._failed_until[key] = time.monotonic() + self._cooldown

    def mark_ok(self, key: str):
        self._failed_until.pop(key, None)


# ===========================================================================
# 调度器
# ===========================================================================
class Scheduler:
    def __init__(self, rate: float, mode: str = "arrival", stop_event: asyncio.Event = None, burst: float = None):
        self.rate = rate
        self.mode = mode
        self.stop_event = stop_event or asyncio.Event()
        self._cond = asyncio.Condition()
        self._queue = asyncio.Queue(maxsize=1024)
        self._task = None
        self.capacity = int(burst if burst is not None else rate) if rate > 0 else 1
        if self.capacity < 1: self.capacity = 1
        self._tokens = float(self.capacity)
        self._last_refill = time.monotonic()
        self._dropped_tokens = 0

        if self.mode == "arrival" and self.rate > 0:
            self._task = asyncio.create_task(self._arrival_loop())

    async def _arrival_loop(self):
        interval = 1.0 / self.rate
        next_time = time.monotonic()
        while not self.stop_event.is_set():
            now = time.monotonic()
            if now < next_time:
                await asyncio.sleep(next_time - now)
            next_time += interval
            if next_time < time.monotonic():
                next_time = time.monotonic() + interval
            try:
                self._queue.put_nowait(True)
            except asyncio.QueueFull:
                self._dropped_tokens += 1
                if self._dropped_tokens % 1000 == 0:
                    _log(f"[WARN] Scheduler queue full, dropped {self._dropped_tokens} tokens.")

    async def acquire(self):
        if self.mode == "arrival" and self.rate > 0:
            await self._queue.get()
        else:
            async with self._cond:
                while not self.stop_event.is_set():
                    if self.rate <= 0: return
                    now = time.monotonic()
                    elapsed = now - self._last_refill
                    self._last_refill = now
                    self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
                    if self._tokens >= 1.0:
                        self._tokens -= 1.0
                        return
                    wait_time = (1.0 - self._tokens) / max(self.rate, 0.001)
                    try: await asyncio.wait_for(self._cond.wait(), timeout=wait_time)
                    except asyncio.TimeoutError: pass

    def stop(self):
        self.stop_event.set()
        if self._task: self._task.cancel()

# ===========================================================================
# 极速 HDR Histogram
# ===========================================================================
class HDRHistogram:
    __slots__ = ("buckets", "count", "min_val", "max_val")
    def __init__(self):
        self.buckets = [0] * 32
        self.count = 0
        self.min_val = float('inf')
        self.max_val = 0.0

    def add(self, ms: float):
        self.count += 1
        if ms < self.min_val: self.min_val = ms
        if ms > self.max_val: self.max_val = ms
        us = int(ms * 1000)
        if us <= 0:
            self.buckets[0] += 1
        else:
            self.buckets[min(31, us.bit_length())] += 1

    def percentile(self, p: float) -> float:
        if self.count == 0: return 0.0
        target = self.count * (p / 100.0)
        c = 0
        for i, b in enumerate(self.buckets):
            c += b
            if c >= target:
                return (1 << i) / 1000.0
        return self.max_val


def merged_percentile(hist: list, count: int, p: float) -> float:
    if count == 0: return 0.0
    target = count * (p / 100.0)
    c = 0
    for i, b in enumerate(hist):
        c += b
        if c >= target:
            return (1 << i) / 1000.0
    return 0.0


# ===========================================================================
# [H4] 统计模块 — __slots__ 普通类, record() 为全程序最热路径
# ===========================================================================
class Stats:
    __slots__ = ("requests", "success", "failed", "bytes_in", "bytes_out",
                 "status_codes", "errors", "lat_sum", "lat_min", "lat_max",
                 "_hist", "_per_endpoint", "_detail_enabled", "_detail_file",
                 "_csv_writer", "_detail_count", "_consecutive_fails",
                 "start_time", "_timeline_rps", "_timeline_lat", "h2_pushes",
                 "_recording", "connect_ms_sum", "connect_count", "warmup_requests")

    def __init__(self):
        self.requests = 0
        self.success = 0
        self.failed = 0
        self.bytes_in = 0
        self.bytes_out = 0
        self.status_codes = {}
        self.errors = defaultdict(int)
        self.lat_sum = 0.0
        self.lat_min = None
        self.lat_max = 0.0
        self._hist = HDRHistogram()
        self._per_endpoint = defaultdict(int)
        self._detail_enabled = False
        self._detail_file = None
        self._csv_writer = None
        self._detail_count = 0
        self._consecutive_fails = 0
        self.start_time = time.time()
        self._timeline_rps = deque(maxlen=3600)
        self._timeline_lat = deque(maxlen=3600)
        self.h2_pushes = 0
        self._recording = True
        self.connect_ms_sum = 0.0
        self.connect_count = 0
        self.warmup_requests = 0

    def enable_detail_log(self, filename: str, fmt: str = 'csv'):
        self._detail_enabled = True
        if fmt == 'csv':
            self._detail_file = open(filename, 'w', encoding='utf-8', newline='')
            self._csv_writer = csv.writer(self._detail_file)
            self._csv_writer.writerow(['timestamp', 'status', 'latency_ms', 'success', 'error', 'endpoint'])
        elif fmt == 'json':
            self._detail_file = open(filename, 'wb')

    def record(self, status=0, bytes_in=0, bytes_out=0, ms=0.0, ok=True,
               err=None, endpoint="", circuit_breaker=0, stop_event=None, h2_push=0):
        if not self._recording:
            self.warmup_requests += 1
            return
        self.requests += 1
        if ok:
            self.success += 1
            self._consecutive_fails = 0
        else:
            self.failed += 1
            self._consecutive_fails += 1
            if circuit_breaker > 0 and self._consecutive_fails >= circuit_breaker:
                if stop_event: stop_event.set()
                _log(f"[FATAL] Circuit breaker tripped: {self._consecutive_fails} consecutive fails.")

        self.h2_pushes += h2_push
        self.bytes_in += bytes_in
        self.bytes_out += bytes_out
        if status:
            self.status_codes[status] = self.status_codes.get(status, 0) + 1
        if err:
            if err in self.errors: self.errors[err] += 1
            elif len(self.errors) < MAX_ERROR_KEYS: self.errors[err] = 1
        if endpoint:
            self._per_endpoint[endpoint] += 1
        self.lat_sum += ms
        if self.lat_min is None or ms < self.lat_min: self.lat_min = ms
        if ms > self.lat_max: self.lat_max = ms
        self._hist.add(ms)
        if self._detail_enabled:
            self._detail_count += 1
            if self._csv_writer:
                self._csv_writer.writerow([time.time(), status, round(ms, 2), ok, err or '', endpoint])
            elif self._detail_file:
                line = json.dumps({'ts': time.time(), 'status': status, 'ms': round(ms, 2), 'ok': ok, 'err': err, 'ep': endpoint})
                self._detail_file.write((line + '\n').encode())
            if self._detail_count % 10000 == 0 and self._detail_file is not None:
                try: self._detail_file.flush()
                except Exception: pass

    def record_connect(self, ms: float):
        """[H7] 新连接建立耗时 (TCP+TLS 握手)"""
        if not self._recording: return
        self.connect_ms_sum += ms
        self.connect_count += 1

    def reset(self):
        """[H5] 预热结束后清零, 正式统计从零开始"""
        self.requests = 0
        self.success = 0
        self.failed = 0
        self.bytes_in = 0
        self.bytes_out = 0
        self.status_codes = {}
        self.errors = defaultdict(int)
        self.lat_sum = 0.0
        self.lat_min = None
        self.lat_max = 0.0
        self._hist = HDRHistogram()
        self._per_endpoint = defaultdict(int)
        self._consecutive_fails = 0
        self.start_time = time.time()
        self.h2_pushes = 0
        self.connect_ms_sum = 0.0
        self.connect_count = 0
        self.warmup_requests = 0
        self._timeline_rps.clear()
        self._timeline_lat.clear()
        self._recording = True

    def record_timeline(self, elapsed: float, rps: float, lat_p50: float, lat_p99: float):
        self._timeline_rps.append((elapsed, rps))
        self._timeline_lat.append((elapsed, lat_p50, lat_p99))

    def snapshot(self, wid=0, final=False) -> dict:
        """累计快照: 集群聚合器只保留每个 wid 的最新一份再求和, 绝不重复计数"""
        return {
            "wid": wid, "final": final,
            "req": self.requests, "ok": self.success, "fail": self.failed,
            "bytes_in": self.bytes_in, "bytes_out": self.bytes_out,
            "lat_sum": self.lat_sum,
            "lat_min": self.lat_min if self.lat_min is not None else -1.0,
            "lat_max": self.lat_max,
            "h2_pushes": self.h2_pushes,
            "count": self._hist.count,
            "hist": list(self._hist.buckets),
            "status_codes": dict(self.status_codes),
            "connect_ms_sum": self.connect_ms_sum,
            "connect_count": self.connect_count,
        }

    @property
    def p50(self): return self._hist.percentile(50)
    @property
    def p95(self): return self._hist.percentile(95)
    @property
    def p99(self): return self._hist.percentile(99)
    @property
    def error_rate(self):
        return self.failed / self.requests if self.requests > 0 else 0.0

    def close_detail(self):
        if self._detail_file:
            try: self._detail_file.flush()
            except Exception: pass
            try: self._detail_file.close()
            except Exception: pass
            self._detail_file = None
# ===========================================================================
# 集群聚合器
# ===========================================================================
class ClusterAggregator:
    """每 Worker 只保留最新累计快照; 汇总 = 所有最新快照之和 (含直方图精确合并)"""
    def __init__(self):
        self.latest: Dict[Any, dict] = {}
        self.registered: set = set()
        self.start_time = time.time()

    def register(self, wid):
        self.registered.add(wid)

    def apply(self, snap: dict):
        wid = snap.get("wid", 0)
        self.latest[wid] = snap

    @property
    def all_done(self):
        return bool(self.latest) and all(s.get("final") for s in self.latest.values())

    def totals(self) -> dict:
        req = ok = fail = bi = bo = pushes = count = 0
        cms = 0.0; cc = 0
        lat_sum = 0.0; lat_min = None; lat_max = 0.0
        hist = [0] * 32
        status_codes = defaultdict(int)
        for s in self.latest.values():
            req += s["req"]; ok += s["ok"]; fail += s["fail"]
            bi += s["bytes_in"]; bo += s["bytes_out"]
            pushes += s["h2_pushes"]; count += s["count"]
            lat_sum += s["lat_sum"]
            lm = s.get("lat_min", -1.0)
            if lm is not None and lm >= 0:
                lat_min = lm if lat_min is None else min(lat_min, lm)
            if s["lat_max"] > lat_max: lat_max = s["lat_max"]
            cms += s.get("connect_ms_sum", 0.0)
            cc += s.get("connect_count", 0)
            h = s.get("hist") or []
            for i in range(min(32, len(h))): hist[i] += h[i]
            for k, v in (s.get("status_codes") or {}).items():
                status_codes[k] += v
        return {"req": req, "ok": ok, "fail": fail, "bytes_in": bi, "bytes_out": bo,
                "lat_sum": lat_sum, "lat_min": lat_min, "lat_max": lat_max,
                "h2_pushes": pushes, "count": count, "hist": hist,
                "status_codes": dict(status_codes),
                "connect_ms_sum": cms, "connect_count": cc}


class MergedStatsView:
    """让 check_thresholds 直接作用于集群合并结果"""
    def __init__(self, t: dict):
        self._t = t
        self.requests = t["req"]
        self.failed = t["fail"]
        self.status_codes = {int(k): v for k, v in t.get("status_codes", {}).items()}

    @property
    def p50(self): return merged_percentile(self._t["hist"], self._t["count"], 50)
    @property
    def p95(self): return merged_percentile(self._t["hist"], self._t["count"], 95)
    @property
    def p99(self): return merged_percentile(self._t["hist"], self._t["count"], 99)
    @property
    def error_rate(self):
        return self.failed / self.requests if self.requests > 0 else 0.0


# ===========================================================================
# YAML 场景解析
# ===========================================================================
def load_yaml_config(filepath: str) -> dict:
    if not HAS_YAML:
        print("[ERROR] YAML 配置需要: pip install pyyaml"); sys.exit(1)
    try:
        with open(filepath, 'r', encoding='utf-8') as f: return yaml.safe_load(f)
    except Exception as e:
        print(f"[ERROR] 解析 YAML 失败: {e}"); sys.exit(1)


# ===========================================================================
# Endpoint / TargetGroup
# ===========================================================================
@dataclass
class Endpoint:
    ip: str
    port: int
    hostname: str
    scheme: str
    alpn: str = ""
    server_hdr: str = ""
    h3_port: int = 0
    is_v6: bool = False
    display: str = ""

    def __post_init__(self):
        proto = "https" if self.scheme == "https" else "http"
        self.display = f"{proto}://{self.ip}:{self.port} [{self.alpn or '?'}]"

    @property
    def conn_host(self):
        return f"[{self.ip}]" if self.is_v6 else self.ip

@dataclass
class TargetGroup:
    hostname: str
    port: int
    scheme: str
    base_path: str
    base_query: str
    method: str
    body: Optional[bytes]
    host_hdr: str
    endpoints: List[Endpoint] = field(default_factory=list)

    def build_path(self):
        qs = random_query_string(self.base_query)
        path_part = self.base_path.split("?", 1)[0] if "?" in self.base_path else self.base_path
        return f"{path_part}?{qs}"

    def build_static_path(self):
        return self.base_path


# ===========================================================================
# 服务器发现
# ===========================================================================
class ServerDiscovery:
    @staticmethod
    async def resolve_dns(hostname: str, use_aiodns: bool = True) -> List[Tuple[str, bool]]:
        results = []
        if use_aiodns and HAS_AIODNS:
            resolver = aiodns.DNSResolver()
            try:
                a_records = await resolver.gethostbyname(hostname, socket.AF_INET)
                if a_records:
                    for addr in a_records.addresses: results.append((addr, False))
            except Exception: pass
            try:
                aaaa_records = await resolver.gethostbyname(hostname, socket.AF_INET6)
                if aaaa_records:
                    for addr in aaaa_records.addresses: results.append((addr, True))
            except Exception: pass
        if not results:
            loop = asyncio.get_event_loop()
            def _resolve():
                addrs = []
                try:
                    infos = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
                    for family, _, _, _, sockaddr in infos:
                        if family == socket.AF_INET: addrs.append((sockaddr[0], False))
                        elif family == socket.AF_INET6: addrs.append((sockaddr[0], True))
                except socket.gaierror: pass
                seen = set(); unique = []
                for ip, v6 in addrs:
                    if ip not in seen: seen.add(ip); unique.append((ip, v6))
                return unique
            results = await loop.run_in_executor(None, _resolve)
        seen = set(); unique = []
        for ip, v6 in results:
            if ip not in seen: seen.add(ip); unique.append((ip, v6))
        return unique

    @staticmethod
    def make_ssl_ctx(hostname: str, insecure: bool = True, alpn: Optional[list] = None) -> ssl.SSLContext:
        ctx = ssl.create_default_context()
        if insecure:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        if alpn: ctx.set_alpn_protocols(alpn)
        return ctx

    @staticmethod
    def _probe_blocking(ip: str, port: int, hostname: str, scheme: str, insecure: bool, timeout: float) -> Endpoint:
        is_v6 = ':' in ip
        ep = Endpoint(ip=ip, port=port, hostname=hostname, scheme=scheme, is_v6=is_v6)
        if scheme != "https":
            ep.alpn = "http/1.1"
            return ep
        tls_sock = None; sock = None
        try:
            ctx = ServerDiscovery.make_ssl_ctx(hostname, insecure, ["h2", "http/1.1"])
            conn_family = socket.AF_INET6 if is_v6 else socket.AF_INET
            sock = socket.socket(conn_family, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            conn_addr = (ip, port) if not is_v6 else (ip, port, 0, 0)
            sock.connect(conn_addr)
            tls_sock = ctx.wrap_socket(sock, server_hostname=hostname)
            sock = None
            negotiated = tls_sock.selected_alpn_protocol()
            ep.alpn = negotiated if negotiated else "http/1.1"
            probe = (f"HEAD / HTTP/1.1\r\nHost: {hostname}\r\nUser-Agent: probe/1.0\r\nConnection: close\r\n\r\n").encode()
            tls_sock.sendall(probe)
            resp_data = b""
            while b"\r\n\r\n" not in resp_data and len(resp_data) < 8192:
                chunk = tls_sock.recv(4096)
                if not chunk: break
                resp_data += chunk
            resp_text = resp_data.decode("iso-8859-1", errors="replace")
            for line in resp_text.split("\r\n"):
                lower = line.lower()
                if lower.startswith("server:") and ":" in line:
                    ep.server_hdr = line.split(":", 1)[1].strip()
                elif lower.startswith("alt-svc:") and ":" in line:
                    alt_svcs = parse_alt_svc(line.split(":", 1)[1].strip())
                    if alt_svcs: ep.h3_port = alt_svcs[0].get('port', 0)
        except Exception as e:
            ep.alpn = "http/1.1"
            ep.server_hdr = f"probe-error: {type(e).__name__}"
        finally:
            for s in (tls_sock, sock):
                if s is not None:
                    try: s.close()
                    except Exception: pass
        return ep

    @staticmethod
    async def probe_endpoint(ip: str, port: int, hostname: str, scheme: str, insecure: bool, timeout: float = 5.0) -> Endpoint:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, ServerDiscovery._probe_blocking, ip, port, hostname, scheme, insecure, timeout)

    @staticmethod
    async def discover(target_url: str, default_path: str, method: str, body: Optional[bytes], insecure: bool, timeout: float = 5.0, prefer_ips: Optional[List[str]] = None) -> TargetGroup:
        if "://" not in target_url: target_url = f"http://{target_url}"
        parsed = urlparse(target_url)
        hostname = parsed.hostname or target_url
        is_tls = (parsed.scheme == "https")
        port = parsed.port or (443 if is_tls else 80)
        scheme = "https" if is_tls else "http"
        if parsed.path:
            base_path = parsed.path
            if parsed.query: base_path += "?" + parsed.query
        else:
            base_path = default_path if default_path.startswith("/") else "/" + default_path
        if (is_tls and port == 443) or (not is_tls and port == 80): host_hdr = hostname
        else: host_hdr = f"{hostname}:{port}"
        base_query = base_path.split("?", 1)[1] if "?" in base_path else ""
        group = TargetGroup(hostname=hostname, port=port, scheme=scheme, base_path=base_path, base_query=base_query, method=method, body=body, host_hdr=host_hdr)
        _log(f"\n[DISCOVERY] {scheme}://{hostname}:{port}\n  Path: {base_path}")

        ip_list = await ServerDiscovery.resolve_dns(hostname, use_aiodns=HAS_AIODNS)
        if prefer_ips: ip_list = [(ip, ':' in ip) for ip in prefer_ips if ip]
        if not ip_list: ip_list = [(hostname, ':' in hostname)]
        _log(f"  DNS resolved: {len(ip_list)} IP(s)")
        for ip, is_v6 in ip_list: _log(f"    {ip}{' (IPv6)' if is_v6 else ''}")

        tasks = [ServerDiscovery.probe_endpoint(ip, port, hostname, scheme, insecure, timeout) for ip, _ in ip_list]
        endpoints = await asyncio.gather(*tasks, return_exceptions=True)
        for ep in endpoints:
            if isinstance(ep, Exception):
                _log(f"  [WARN] probe failed: {ep}"); continue
            if ep:
                group.endpoints.append(ep)
                h3_info = f", h3_port={ep.h3_port}" if ep.h3_port else ""
                _log(f"  Endpoint: {ep.display}  server={ep.server_hdr or '?'}{h3_info}")
        if not group.endpoints:
            ep = Endpoint(ip=hostname, port=port, hostname=hostname, scheme=scheme, is_v6=(':' in hostname), alpn="http/1.1")
            group.endpoints.append(ep)
            _log(f"  [FALLBACK] using hostname directly: {ep.display}")
        _log(f"  Total endpoints: {len(group.endpoints)}")
        return group

# ===========================================================================
# TCP 层干扰
# ===========================================================================
class TCPInterference:
    def __init__(self, args):
        self.sndbuf = args.tcp_sndbuf
        self.rcvbuf = args.tcp_rcvbuf
        self.window_clamp = args.tcp_window_clamp
        self.nodelay = not args.tcp_nagle
        self.linger_rst = args.tcp_linger_rst
        self.churn_interval = args.tcp_churn
        self.small_segments = args.tcp_small_segs
        self.ack_inject = args.tcp_ack_inject

    def apply(self, sock):
        if self.sndbuf is not None:
            try: sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.sndbuf)
            except OSError: pass
        if self.rcvbuf is not None:
            try: sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.rcvbuf)
            except OSError: pass
        try: sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1 if self.nodelay else 0)
        except OSError: pass
        if self.window_clamp is not None:
            clamp = getattr(socket, "TCP_WINDOW_CLAMP", None)
            if clamp is not None:
                try: sock.setsockopt(socket.IPPROTO_TCP, clamp, self.window_clamp)
                except OSError: pass
        if self.linger_rst:
            try: sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            except OSError: pass

class RawACKInjector:
    def __init__(self):
        self.sock = None
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
        except (PermissionError, OSError) as e:
            _log(f"[DEBUG] RawACK init failed: {e}"); self.sock = None
        self._rng = random.Random()

    @property
    def available(self): return self.sock is not None

    @staticmethod
    def _checksum(data: bytes) -> int:
        if len(data) % 2: data += b"\x00"
        s = 0
        for i in range(0, len(data), 2): s += (data[i] << 8) + data[i + 1]
        s = (s >> 16) + (s & 0xffff); s += (s >> 16)
        return (~s) & 0xffff

    def inject_ack(self, local_ip: str, remote_ip: str, src_port: int, dst_port: int, ack_seq: int = 0):
        if self.sock is None: return
        if ':' in local_ip or ':' in remote_ip: return
        try:
            src = socket.inet_aton(local_ip); dst = socket.inet_aton(remote_ip)
        except OSError: return
        seq = self._rng.randint(0, 0xFFFFFFFF); ack = ack_seq & 0xFFFFFFFF
        ip_hdr = struct.pack("!BBHHHBBH4s4s", (4 << 4) | 5, 0, 40, self._rng.randint(0, 65535), 0x4000, 64, socket.IPPROTO_TCP, 0, src, dst)
        pseudo = src + dst + struct.pack("!BBH", 0, socket.IPPROTO_TCP, 20)
        tcp_hdr = struct.pack("!HHBBHHH", src_port, dst_port, seq, ack, (5 << 4), 0x10, 65535) + struct.pack("!HH", 0, 0)
        chk = self._checksum(pseudo + tcp_hdr)
        tcp_hdr = struct.pack("!HHBBHHH", src_port, dst_port, seq, ack, (5 << 4), 0x10, 65535) + struct.pack("!HH", chk, 0)
        try: self.sock.sendto(ip_hdr + tcp_hdr, (remote_ip, 0))
        except OSError: pass

    def close(self):
        if self.sock: self.sock.close(); self.sock = None


# ===========================================================================
# HTTP/1.1 响应解析 (扫描游标 + 64KB 大块读)
# ===========================================================================
async def read_h1_response(reader):
    buf = bytearray()
    scan = 0
    hdr_end = -1
    while hdr_end == -1:
        chunk = await reader.read(65536)
        if not chunk:
            raise asyncio.IncompleteReadError(b"", None)
        buf.extend(chunk)
        hdr_end = buf.find(b"\r\n\r\n", scan)
        if hdr_end == -1:
            scan = max(0, len(buf) - 3)
            if len(buf) > MAX_H1_HEADER:
                break
    if hdr_end >= 0:
        header_data = bytes(buf[:hdr_end + 4])
        leftover = bytes(buf[hdr_end + 4:])
    else:
        header_data = bytes(buf); leftover = b""
    head = header_data.decode("iso-8859-1", errors="replace")
    lines = head.split("\r\n")
    try: status = int(lines[0].split(" ", 2)[1])
    except (IndexError, ValueError): status = 0
    chunked = False; clen = None; close_conn = False
    for line in lines[1:]:
        if not line or ":" not in line: continue
        k, v = line.split(":", 1)
        k = k.strip().lower(); v = v.strip()
        if k == "transfer-encoding" and "chunked" in v.lower(): chunked = True
        elif k == "content-length":
            try: clen = int(v)
            except ValueError: pass
        elif k == "connection" and "close" in v.lower(): close_conn = True
    return status, chunked, clen, close_conn, leftover

async def drain_h1_body(reader, chunked, clen, leftover=b"", max_drain=128*1024*1024):
    if chunked:
        buf = bytearray(leftover); drained = 0; scan = 0
        while drained < max_drain:
            while True:
                idx = buf.find(b"\r\n", scan)
                if idx != -1: break
                scan = max(0, len(buf) - 1)
                chunk = await reader.read(65536)
                if not chunk: return drained
                buf.extend(chunk)
                if len(buf) > MAX_H1_CHUNK_LINE: return drained
            size_line = bytes(buf[:idx]); del buf[:idx + 2]; scan = 0
            try: size = int(size_line.strip().split(b";", 1)[0], 16)
            except ValueError: return drained
            if size == 0:
                while True:
                    idx = buf.find(b"\r\n")
                    if idx == -1:
                        chunk = await reader.read(8192)
                        if not chunk: return drained
                        buf.extend(chunk)
                        continue
                    line = bytes(buf[:idx]); del buf[:idx + 2]
                    if not line: break
                return drained
            remaining = size + 2
            while remaining > 0:
                if buf:
                    take = min(len(buf), remaining); del buf[:take]; remaining -= take
                else:
                    chunk = await reader.read(min(remaining, 65536))
                    if not chunk: return drained
                    remaining -= len(chunk)
            drained += size
        return drained
    if clen is not None:
        remaining = min(clen, max_drain)
        if leftover:
            take = min(len(leftover), remaining); remaining -= take
        while remaining > 0:
            chunk = await reader.read(min(remaining, 65536))
            if not chunk: break
            remaining -= len(chunk)
        return min(clen, max_drain)
    drained = len(leftover)
    while drained < max_drain:
        chunk = await reader.read(65536)
        if not chunk: break
        drained += len(chunk)
    return drained

# ===========================================================================
# 连接池
# ===========================================================================
class ConnPool:
    def __init__(self, max_per_ep: int = 64):
        self._pools: Dict[str, asyncio.LifoQueue] = {}
        self._max = max_per_ep

    def get(self, key) -> Optional[tuple]:
        pool = self._pools.get(key)
        if not pool: return None
        try:
            _, conn = pool.get_nowait()
            reader, writer = conn
            if not reader.at_eof(): return conn
            try: writer.close()
            except Exception: pass
        except asyncio.QueueEmpty:
            return None
        return None

    def put(self, key, conn: tuple):
        reader, writer = conn
        if reader.at_eof():
            try: writer.close()
            except Exception: pass
            return
        pool = self._pools.setdefault(key, asyncio.LifoQueue(maxsize=self._max))
        try:
            pool.put_nowait((id(conn), conn))
        except asyncio.QueueFull:
            try: writer.close()
            except Exception: pass

    def discard(self, conn: tuple):
        """只丢弃出错的那一条连接, 不牵连整个端点连接池"""
        try: conn[1].close()
        except Exception: pass

    def evict_all(self, key):
        pool = self._pools.pop(key, None)
        if pool:
            while not pool.empty():
                try:
                    _, (_, w) = pool.get_nowait()
                    try: w.close()
                    except Exception: pass
                except asyncio.QueueEmpty:
                    break


# ===========================================================================
# Raw 请求预编译器
# ===========================================================================
class RawRequestPrecompiler:
    """预编译请求字节模板: 随机 header 变体池 + 每组静态前缀, 热路径仅剩 bytes 拼接"""
    def __init__(self, groups, args, base_extra, variant_count=256):
        self._rng = random.Random()
        self.variants = []
        for _ in range(variant_count):
            if args.randomize:
                hdrs = random_header_set()
                for k, v in base_extra.items():
                    if k.lower() not in ("user-agent", "accept-language",
                                         "cache-control", "pragma", "accept"):
                        hdrs[k] = v
            else:
                hdrs = base_extra
            lines = [f"{k}: {v}" for k, v in hdrs.items()]
            self.variants.append(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1"))

        has_host = any(k.lower() == "host" for k in base_extra)
        self._need_ct = not any(k.lower() == "content-type" for k in base_extra)

        self.group_meta = {}
        for g in groups:
            static = b""
            if g.body is not None:
                if self._need_ct:
                    static += b"Content-Type: application/octet-stream\r\n"
                static += f"Content-Length: {len(g.body)}\r\n".encode()
            self.group_meta[id(g)] = {
                "method": f"{g.method} ".encode("latin-1"),
                "host": b"" if has_host else f"Host: {g.host_hdr}\r\n".encode("latin-1"),
                "static": static,
                "body": g.body or b"",
            }

    def build(self, group, path, body_override=None) -> bytes:
        meta = self.group_meta[id(group)]
        v = self.variants[self._rng.randrange(len(self.variants))]
        path_b = path.encode("utf-8", "replace")
        if body_override is None:
            return (meta["method"] + path_b + b" HTTP/1.1\r\n"
                    + meta["host"] + meta["static"] + v + meta["body"])
        cl = f"Content-Length: {len(body_override)}\r\n".encode()
        ct = b"Content-Type: application/octet-stream\r\n" if self._need_ct else b""
        return (meta["method"] + path_b + b" HTTP/1.1\r\n"
                + meta["host"] + ct + cl + v + body_override)


# ===========================================================================
# Raw HTTP/1.1 引擎
# ===========================================================================
async def _raw_request_session(reader, writer, req_bytes, group, args, ep, small_segs=False):
    if small_segs:
        # [H8] 小分段干扰: 每次只写 1~16 字节并强制 flush
        pos = 0; total = len(req_bytes)
        while pos < total:
            take = random.randint(1, 16)
            writer.write(req_bytes[pos:pos + take])
            await writer.drain()
            pos += take
    else:
        writer.write(req_bytes)
        await writer.drain()
    status, chunked, clen, close_flag, leftover = await read_h1_response(reader)
    if status in (204, 304) or group.method.upper() == "HEAD": body_len = 0
    else:
        body_len = await drain_h1_body(reader, chunked, clen, leftover)
        if args.skip_body: body_len = 0
    return status, body_len, close_flag

async def raw_worker(groups, args, ssl_ctx, tcp_intf, ack_injector, health, stats, stop_event, scheduler, data_pool=None):
    pool = ConnPool(max_per_ep=getattr(args, 'max_conns_per_ep', 64))
    rnd = random.Random(); idx = 0; req_count = 0
    all_eps = [ep for g in groups for ep in g.endpoints]
    n = len(all_eps)
    if n == 0: return

    ep_group = build_ep_group_map(groups)
    base_extra = build_headers(args.header, args)
    pre = RawRequestPrecompiler(groups, args, base_extra)
    small_segs = bool(tcp_intf.small_segments)

    ep_ssl_ctxs = {}
    for ep in all_eps:
        if ep.scheme == "https":
            ep_ssl_ctxs[(ep.ip, ep.hostname)] = ServerDiscovery.make_ssl_ctx(ep.hostname, args.insecure, ["h2", "http/1.1"])

    async def get_conn(ep):
        """返回 (reader, writer, reused)"""
        key = f"{ep.ip}:{ep.port}"
        if not health.ready(key): raise ConnectionError(f"endpoint {key} cooling down")
        conn = pool.get(key)
        if conn is not None:
            return conn[0], conn[1], True
        conn_family = socket.AF_INET6 if ep.is_v6 else socket.AF_INET
        sock = socket.socket(conn_family, socket.SOCK_STREAM)
        tcp_intf.apply(sock)
        use_ssl = ep.scheme == "https"
        coro = asyncio.open_connection(ep.ip, ep.port,
                                       ssl=ep_ssl_ctxs.get((ep.ip, ep.hostname)) if use_ssl else None,
                                       sock=sock, server_hostname=ep.hostname if use_ssl else None)
        t0 = time.perf_counter()
        try:
            reader, writer = await _run_timed(coro, args.timeout)
            health.mark_ok(key)
        except Exception:
            health.mark_fail(key)
            try: sock.close()
            except Exception: pass
            raise
        stats.record_connect((time.perf_counter() - t0) * 1000.0)  # [H7]
        return reader, writer, False

    try:
        while not stop_event.is_set():
            if scheduler: await scheduler.acquire()
            ep = all_eps[rnd.randrange(n)] if args.mode == "random" else all_eps[idx % n]
            idx += 1
            group = ep_group.get(id(ep), groups[0])
            key = f"{ep.ip}:{ep.port}"

            data_ctx = data_pool.get_next() if data_pool else None
            path = group.build_path() if args.randomize else group.build_static_path()
            if data_ctx and args.format: path = render_template(args.format, data_ctx)

            body_data = group.body
            if data_ctx and args.body:
                body_data = render_template(args.body, data_ctx).encode()
                req_bytes = pre.build(group, path, body_data)
            else:
                req_bytes = pre.build(group, path)

            # [H2] 陈旧复用连接允许一次静默重试
            retried = False
            while True:
                start = time.perf_counter()
                conn = None
                reused = False
                try:
                    reader, writer, reused = await get_conn(ep)

                    if ack_injector and ack_injector.available:
                        try:
                            sockname = writer.get_extra_info('sockname')
                            if sockname:
                                ack_injector.inject_ack(sockname[0], ep.ip, sockname[1], ep.port, random.randint(0, 0xFFFFFFFF))
                        except Exception: pass

                    status, body_len, close_flag = await _run_timed(
                        _raw_request_session(reader, writer, req_bytes, group, args, ep, small_segs),
                        args.timeout)

                    ms = (time.perf_counter() - start) * 1000.0
                    ok = True if args.ignore_status else (status < 400 if status else False)
                    stats.record(status=status, bytes_in=body_len, bytes_out=len(req_bytes), ms=ms, ok=ok,
                                 endpoint=ep.display, circuit_breaker=args.circuit_breaker, stop_event=stop_event)
                    if close_flag:
                        pool.discard(conn)
                    else:
                        pool.put(key, conn)
                    conn = None
                    break
                except asyncio.CancelledError:
                    if conn: pool.discard(conn)
                    raise
                except Exception as e:
                    if conn: pool.discard(conn)
                    conn = None
                    # [H2 FIX] 复用连接的陈旧错误: 静默重试一次, 不污染统计
                    if (not retried and reused and is_conn_level_error(e)
                            and not stop_event.is_set()):
                        retried = True
                        continue
                    ms = (time.perf_counter() - start) * 1000.0
                    stats.record(status=0, ms=ms, ok=False, err=type(e).__name__, endpoint=ep.display,
                                 circuit_breaker=args.circuit_breaker, stop_event=stop_event)
                    break

            req_count += 1
            # [H1 FIX] 仅在显式指定 --tcp-churn 时才周期性清池 (旧版默认 100 会摧毁 keepalive)
            if tcp_intf.churn_interval > 0 and req_count % tcp_intf.churn_interval == 0:
                for g in groups:
                    for e in g.endpoints: pool.evict_all(f"{e.ip}:{e.port}")
    finally:
        for g in groups:
            for e in g.endpoints: pool.evict_all(f"{e.ip}:{e.port}")

# ===========================================================================
# HTTP/2 连接
# ===========================================================================
class H2Connection:
    def __init__(self, reader, writer, ep, group, ssl_ctx, timeout: float = 10.0):
        self.reader = reader; self.writer = writer; self.ep = ep; self.group = group; self._timeout = timeout
        config = h2.config.H2Configuration(client_side=True, header_encoding='utf-8')
        self.conn = h2.connection.H2Connection(config=config)
        self.next_stream_id = 1
        self._conn_lock = asyncio.Lock()
        self._streams: Dict[int, asyncio.Queue] = {}
        self._window_events: Dict[int, asyncio.Event] = {}
        self._max_concurrent = 100
        self._active_streams = 0
        self._stream_cond = asyncio.Condition()
        self._receiver_task = None
        self._closed = False
        self._conn_window_consumed = 0

    async def init(self):
        async with self._conn_lock:
            self.conn.initiate_connection()
            try:
                self.conn.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: H2_INIT_WINDOW, h2.settings.SettingCodes.MAX_FRAME_SIZE: H2_MAX_FRAME})
            except Exception: pass
            data = self.conn.data_to_send()
            if data: self.writer.write(data)
        try: await self.writer.drain()
        except Exception: raise ConnectionError("h2 drain failed during init")
        self._receiver_task = asyncio.create_task(self._receiver_loop())

    async def _notify_all_waiters(self):
        try:
            async with self._stream_cond: self._stream_cond.notify_all()
        except Exception: pass

    def _wake_window_waiters(self):
        for ev in list(self._window_events.values()): ev.set()

    async def _receiver_loop(self):
        try:
            while not self._closed:
                chunk = await self.reader.read(65536)
                if not chunk: break
                data_to_send = None
                events_to_dispatch = []
                async with self._conn_lock:
                    events = self.conn.receive_data(chunk)
                    for event in events:
                        stream_id = getattr(event, 'stream_id', None)
                        if isinstance(event, h2.events.ResponseReceived): events_to_dispatch.append((self._streams.get(stream_id), event))
                        elif isinstance(event, h2.events.DataReceived):
                            self.conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                            self._conn_window_consumed += event.flow_controlled_length
                            if self._conn_window_consumed > H2_WINDOW_ACK_THRESHOLD:
                                try: self.conn.increment_flow_control_window(self._conn_window_consumed); self._conn_window_consumed = 0
                                except Exception: pass
                            events_to_dispatch.append((self._streams.get(stream_id), event))
                        elif isinstance(event, h2.events.StreamEnded): events_to_dispatch.append((self._streams.get(stream_id), event))
                        elif isinstance(event, h2.events.StreamReset): events_to_dispatch.append((self._streams.get(stream_id), event))
                        elif isinstance(event, h2.events.RemoteSettingsChanged):
                            new_max = event.changed_settings.get(h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS)
                            if new_max and new_max.new_value: self._max_concurrent = new_max.new_value
                        elif isinstance(event, h2.events.WindowUpdated):
                            if event.stream_id == 0:
                                for sid, ev in list(self._window_events.items()):
                                    try:
                                        if self.conn.local_flow_control_window(sid) > 0: ev.set()
                                    except Exception: ev.set()
                            else:
                                ev = self._window_events.get(event.stream_id)
                                if ev is not None: ev.set()
                        elif isinstance(event, h2.events.PushedStreamReceived):
                            parent_q = self._streams.get(event.parent_stream_id)
                            if parent_q: events_to_dispatch.append((parent_q, event))
                        elif isinstance(event, h2.events.ConnectionTerminated): self._closed = True; break
                    data_to_send = self.conn.data_to_send()
                    if data_to_send: self.writer.write(data_to_send)
                for q, event in events_to_dispatch:
                    if q is not None: self._safe_put(q, event)
                if data_to_send:
                    try: await self.writer.drain()
                    except Exception: self._closed = True; break
        except asyncio.CancelledError: raise
        except Exception: pass
        finally:
            self._closed = True
            for q in list(self._streams.values()): self._safe_put(q, None)
            self._wake_window_waiters()
            await self._notify_all_waiters()

    @staticmethod
    def _safe_put(q: asyncio.Queue, event):
        try: q.put_nowait(event); return
        except asyncio.QueueFull: pass
        if event is None or isinstance(event, (h2.events.StreamEnded, h2.events.StreamReset)):
            try: q.get_nowait()
            except asyncio.QueueEmpty: pass
            try: q.put_nowait(event)
            except asyncio.QueueFull: pass

    async def send_request(self, path, headers, body=None, skip_body: bool = False) -> Tuple[int, int, int, int]:
        async with self._stream_cond:
            while self._active_streams >= self._max_concurrent and not self._closed:
                try: await asyncio.wait_for(self._stream_cond.wait(), timeout=1.0)
                except asyncio.TimeoutError: pass
            if self._closed: raise ConnectionError("h2 connection closed")
            self._active_streams += 1

        stream_id = None; completed = False; win_ev = None; pushes = 0
        start_time = time.monotonic()
        try:
            async with self._conn_lock:
                if self._closed: raise ConnectionError("h2 connection closed")
                stream_id = self.next_stream_id; self.next_stream_id += 2
                self._streams[stream_id] = asyncio.Queue(maxsize=256)

                req_headers = [(":method", self.group.method), (":path", path), (":scheme", self.ep.scheme), (":authority", self.group.host_hdr)]
                for k, v in headers.items():
                    if k.lower() not in ("host", "connection", "transfer-encoding", "content-length", "keep-alive"): req_headers.append((k, v))
                if body is not None:
                    req_headers.append(("content-length", str(len(body))))
                    win_ev = asyncio.Event(); self._window_events[stream_id] = win_ev

                self.conn.send_headers(stream_id, req_headers)
                if not body: self.conn.end_stream(stream_id)
                data = self.conn.data_to_send()
                if data: self.writer.write(data)
            try: await self.writer.drain()
            except Exception: raise ConnectionError("h2 drain failed during headers")

            if body and win_ev is not None:
                pos = 0; total = len(body)
                body_view = memoryview(body)
                while pos < total:
                    sent = False
                    async with self._conn_lock:
                        if self._closed: raise ConnectionError("h2 connection closed")
                        try:
                            window = self.conn.local_flow_control_window(stream_id)
                            if window > 0:
                                take = min(window, H2_SEND_CHUNK, total - pos)
                                end = (pos + take >= total)
                                self.conn.send_data(stream_id, body_view[pos:pos + take], end_stream=end)
                                pos += take; sent = True
                                data = self.conn.data_to_send()
                                if data: self.writer.write(data)
                        except h2.exceptions.FlowControlError: sent = False
                    try: await self.writer.drain()
                    except Exception: raise ConnectionError("h2 drain failed during body")
                    if not sent:
                        remaining = self._timeout - (time.monotonic() - start_time)
                        if remaining <= 0: raise asyncio.TimeoutError("h2 flow-control window stalled")
                        try: await asyncio.wait_for(win_ev.wait(), timeout=min(1.0, remaining))
                        except asyncio.TimeoutError: pass
                        if self._closed: raise ConnectionError("h2 connection closed during wait")
                        win_ev.clear()

            status = 0; resp_len = 0; body_complete = False
            while not body_complete:
                remaining = self._timeout - (time.monotonic() - start_time)
                if remaining <= 0: break
                try: event = await asyncio.wait_for(self._streams[stream_id].get(), timeout=remaining)
                except asyncio.TimeoutError: break
                if event is None: break
                if isinstance(event, h2.events.ResponseReceived):
                    for k, v in event.headers:
                        if k == ":status":
                            try: status = int(v)
                            except ValueError: pass
                elif isinstance(event, h2.events.DataReceived):
                    resp_len += event.flow_controlled_length
                elif isinstance(event, h2.events.PushedStreamReceived):
                    pushes += 1
                elif isinstance(event, (h2.events.StreamEnded, h2.events.StreamReset)): body_complete = True
            completed = True
            return status, resp_len, resp_len, pushes
        finally:
            if stream_id is not None:
                self._streams.pop(stream_id, None)
                self._window_events.pop(stream_id, None)
                if not completed and not self._closed:
                    async with self._conn_lock:
                        try:
                            self.conn.reset_stream(stream_id)
                            data = self.conn.data_to_send()
                            if data: self.writer.write(data)
                        except Exception: pass
                try: await self.writer.drain()
                except Exception: pass
            try:
                async with self._stream_cond:
                    self._active_streams -= 1; self._stream_cond.notify(1)
            except Exception: pass

    async def close(self):
        self._closed = True
        for q in list(self._streams.values()): self._safe_put(q, None)
        self._wake_window_waiters(); await self._notify_all_waiters()
        if self._receiver_task:
            self._receiver_task.cancel()
            try: await self._receiver_task
            except (asyncio.CancelledError, Exception): pass
        async with self._conn_lock:
            try:
                self.conn.close_connection()
                data = self.conn.data_to_send()
                if data: self.writer.write(data)
            except Exception: pass
        try: await self.writer.drain()
        except Exception: pass
        try: self.writer.close()
        except Exception: pass

# ===========================================================================
# H2 共享连接池
# ===========================================================================
class H2Pool:
    def __init__(self, groups, args, health, stats=None):
        self._groups = groups; self._args = args; self._health = health; self._stats = stats
        self._conns: Dict[str, List[H2Connection]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._max_conns = getattr(args, 'h2_conns_per_ep', 4)
        self._ep_group = build_ep_group_map(groups)
        self._ssl_ctxs: Dict[Tuple[str, str], ssl.SSLContext] = {}

    def group_of(self, ep): return self._ep_group.get(id(ep), self._groups[0])

    def _lock_for(self, key: str) -> asyncio.Lock:
        if key not in self._locks: self._locks[key] = asyncio.Lock()
        return self._locks[key]

    def _ssl_ctx_for(self, ep) -> ssl.SSLContext:
        key = (ep.ip, ep.hostname)
        ctx = self._ssl_ctxs.get(key)
        if ctx is None:
            ctx = ServerDiscovery.make_ssl_ctx(ep.hostname, self._args.insecure, ["h2", "http/1.1"])
            self._ssl_ctxs[key] = ctx
        return ctx

    async def _connect(self, ep) -> H2Connection:
        t0 = time.perf_counter()
        conn_family = socket.AF_INET6 if ep.is_v6 else socket.AF_INET
        sock = socket.socket(conn_family, socket.SOCK_STREAM)
        try: sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError: pass
        try:
            ep_ctx = self._ssl_ctx_for(ep)
            coro = asyncio.open_connection(ep.ip, ep.port, ssl=ep_ctx, sock=sock, server_hostname=ep.hostname)
            reader, writer = await asyncio.wait_for(coro, timeout=self._args.timeout)
        except Exception:
            sock.close(); raise
        conn = H2Connection(reader, writer, ep, self.group_of(ep), ep_ctx, timeout=self._args.timeout)
        await conn.init()
        if self._stats is not None:
            self._stats.record_connect((time.perf_counter() - t0) * 1000.0)  # [H7]
        return conn

    async def get(self, ep) -> H2Connection:
        key = f"{ep.ip}:{ep.port}"
        if not self._health.ready(key): raise ConnectionError(f"endpoint {key} cooling down")
        picked = None; dead: List[H2Connection] = []
        async with self._lock_for(key):
            if not self._health.ready(key): raise ConnectionError(f"endpoint {key} cooling down")
            conns = self._conns.setdefault(key, [])
            alive = []
            for c in conns:
                if c._closed or c.reader.at_eof(): dead.append(c)
                else: alive.append(c)
            conns[:] = alive
            for c in conns:
                if c._active_streams < c._max_concurrent: picked = c; break
            if picked is None and (len(conns) < self._max_conns or not conns):
                try:
                    picked = await self._connect(ep); conns.append(picked); self._health.mark_ok(key)
                except Exception:
                    self._health.mark_fail(key); raise
            if picked is None and conns: picked = min(conns, key=lambda c: c._active_streams)
        for c in dead:
            try: await c.close()
            except Exception: pass
        return picked

    async def invalidate_conn(self, ep, conn):
        if conn is None: return
        key = f"{ep.ip}:{ep.port}"
        async with self._lock_for(key):
            conns = self._conns.get(key, [])
            if conn in conns: conns.remove(conn)
        try: await conn.close()
        except Exception: pass

    async def close_all(self):
        all_conns = []
        for conns in self._conns.values(): all_conns.extend(conns)
        self._conns.clear()
        for conn in all_conns:
            try: await conn.close()
            except Exception: pass

async def h2_worker(pool: H2Pool, groups, args, stats, stop_event, scheduler, data_pool=None):
    rnd = random.Random(); idx = 0
    all_eps = [ep for g in groups for ep in g.endpoints if ep.alpn == "h2"]
    if not all_eps: _log("[WARN] 没有 h2 端点, h2 引擎无法工作"); return
    n = len(all_eps); base_extra = build_headers(args.header, args)

    hdr_variants = []
    for _ in range(64):
        if args.randomize:
            hdrs = random_header_set()
            for k, v in base_extra.items():
                if k.lower() not in ("user-agent", "accept-language", "cache-control", "pragma", "accept"): hdrs[k] = v
        else:
            hdrs = dict(base_extra)
        hdr_variants.append(hdrs)

    while not stop_event.is_set():
        if scheduler: await scheduler.acquire()
        ep = all_eps[rnd.randrange(n)] if args.mode == "random" else all_eps[idx % n]
        idx += 1
        group = pool.group_of(ep)

        data_ctx = data_pool.get_next() if data_pool else None
        path = group.build_path() if args.randomize else group.build_static_path()
        if data_ctx and args.format: path = render_template(args.format, data_ctx)
        hdrs = hdr_variants[rnd.randrange(len(hdr_variants))]

        body_data = group.body
        if data_ctx and args.body: body_data = render_template(args.body, data_ctx).encode()

        start = time.perf_counter(); conn = None
        try:
            conn = await pool.get(ep)
            # [H3] 免 Task 分配的超时
            status, resp_len, _, pushes = await _run_timed(
                conn.send_request(path, hdrs, body_data, skip_body=args.skip_body), args.timeout)
            ms = (time.perf_counter() - start) * 1000.0
            ok = True if args.ignore_status else (status < 400 if status else False)
            stats.record(status=status, bytes_in=resp_len, ms=ms, ok=ok, endpoint=ep.display,
                         circuit_breaker=args.circuit_breaker, stop_event=stop_event, h2_push=pushes)
        except asyncio.CancelledError: raise
        except Exception as e:
            ms = (time.perf_counter() - start) * 1000.0
            stats.record(status=0, ms=ms, ok=False, err=type(e).__name__, endpoint=ep.display,
                         circuit_breaker=args.circuit_breaker, stop_event=stop_event)
            if is_conn_level_error(e):
                try: await pool.invalidate_conn(ep, conn)
                except Exception: pass

# ===========================================================================
# HTTP/3 客户端
# ===========================================================================
class H3Client:
    def __init__(self, ep, port: int, insecure: bool, timeout: float):
        self._ep = ep; self._port = port; self._insecure = insecure; self._timeout = timeout
        self._send_q: asyncio.Queue = asyncio.Queue(maxsize=1024)
        self._wakeup = asyncio.Event(); self._task: Optional[asyncio.Task] = None
        self._closed = False; self._connected = asyncio.Event(); self._conn_lock = asyncio.Lock()

    @staticmethod
    def _safe_put(q: asyncio.Queue, item):
        try: q.put_nowait(item)
        except asyncio.QueueFull:
            try: q.get_nowait()
            except asyncio.QueueEmpty: pass
            try: q.put_nowait(item)
            except asyncio.QueueFull: pass

    async def _ensure_running(self):
        async with self._conn_lock:
            need_start = (self._task is None or self._task.done() or self._closed)
            if need_start:
                self._closed = False; self._connected.clear()
                while True:
                    try: _, _, q = self._send_q.get_nowait()
                    except asyncio.QueueEmpty: break
                    self._safe_put(q, None)
                self._task = asyncio.create_task(self._run())
            if not self._connected.is_set():
                try: await asyncio.wait_for(self._connected.wait(), timeout=self._timeout)
                except asyncio.TimeoutError:
                    if self._task is None or self._task.done(): raise ConnectionError("h3 connect failed (task dead)")
                    raise
            if self._task is None or self._task.done(): raise ConnectionError("h3 connection unavailable")

    async def _run(self):
        config = QuicConfiguration(is_client=True, alpn_protocols=["h3"])
        if self._insecure: config.verify_mode = ssl.CERT_NONE
        pending: Dict[int, Tuple[asyncio.Queue, float]] = {}
        try:
            async with quic_connect(self._ep.ip, self._port, configuration=config, server_hostname=self._ep.hostname) as protocol:
                h3 = H3Connection(protocol._quic); self._connected.set()
                recv_ev = getattr(protocol, '_quic_receive_event', None)

                while not self._closed:
                    while True:
                        try: req = self._send_q.get_nowait()
                        except asyncio.QueueEmpty: break
                        headers, body, resp_q = req
                        try:
                            stream_id = h3.get_next_available_stream_id()
                            h3.send_headers(stream_id, headers, end_stream=body is None)
                            if body is not None: h3.send_data(stream_id, body, end_stream=True)
                            pending[stream_id] = (resp_q, time.monotonic())
                        except Exception: self._safe_put(resp_q, None)
                    protocol.transmit()

                    for event in h3.get_events():
                        sid = getattr(event, 'stream_id', None)
                        entry = pending.get(sid)
                        if entry is None: continue
                        q = entry[0]; self._safe_put(q, event)
                        if isinstance(event, StreamEnded):
                            pending.pop(sid, None); self._safe_put(q, None)
                    protocol.transmit()

                    now = time.monotonic()
                    expired = [s for s, (_, ts) in pending.items() if now - ts > self._timeout * 1.5]
                    for s in expired:
                        q, _ = pending.pop(s); self._safe_put(q, None)

                    if recv_ev is not None: poll_timeout = None
                    elif pending: poll_timeout = 0.05
                    else: poll_timeout = 0.2

                    wakers = [asyncio.ensure_future(self._wakeup.wait())]
                    if recv_ev is not None: wakers.append(asyncio.ensure_future(recv_ev.wait()))
                    try:
                        await asyncio.wait(wakers, timeout=poll_timeout, return_when=asyncio.FIRST_COMPLETED)
                    finally:
                        for t in wakers: t.cancel()
                        await asyncio.gather(*wakers, return_exceptions=True)
                        self._wakeup.clear()
                        if recv_ev is not None: recv_ev.clear()
        except asyncio.CancelledError: raise
        except Exception: pass
        finally:
            self._closed = True
            while True:
                try: _, _, q = self._send_q.get_nowait()
                except asyncio.QueueEmpty: break
                self._safe_put(q, None)
            for q, _ in list(pending.values()): self._safe_put(q, None)
            self._connected.set()

    async def request(self, req_headers, body) -> Tuple[int, int]:
        await self._ensure_running()
        resp_q: asyncio.Queue = asyncio.Queue(maxsize=128)
        try: await asyncio.wait_for(self._send_q.put((req_headers, body, resp_q)), timeout=self._timeout)
        except asyncio.TimeoutError: raise ConnectionError("h3 send queue full")
        self._wakeup.set()

        status = 0; body_len = 0
        deadline = time.monotonic() + self._timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0: raise asyncio.TimeoutError()
            try: ev = await asyncio.wait_for(resp_q.get(), timeout=remaining)
            except asyncio.TimeoutError: raise
            if ev is None: break
            if isinstance(ev, HeadersReceived):
                for k, v in ev.headers:
                    if k == b":status":
                        try: status = int(v.decode())
                        except (ValueError, UnicodeDecodeError): pass
            elif isinstance(ev, DataReceived): body_len += len(ev.data)
            elif isinstance(ev, StreamEnded): break
        return status, body_len

    def stop(self):
        self._closed = True; self._wakeup.set()
        if self._task is not None and not self._task.done(): self._task.cancel()

class H3Pool:
    def __init__(self, args, health):
        self._args = args; self._health = health; self._clients: Dict[str, H3Client] = {}; self._locks: Dict[str, asyncio.Lock] = {}

    async def get(self, ep, port) -> H3Client:
        key = f"{ep.ip}:{port}"
        if not self._health.ready(key): raise ConnectionError(f"endpoint {key} cooling down")
        if key not in self._locks: self._locks[key] = asyncio.Lock()
        async with self._locks[key]:
            client = self._clients.get(key)
            if client is None:
                client = H3Client(ep, port, self._args.insecure, self._args.timeout)
                self._clients[key] = client
            return client

    async def invalidate(self, ep, port):
        key = f"{ep.ip}:{port}"
        async with self._locks.setdefault(key, asyncio.Lock()):
            client = self._clients.pop(key, None)
        if client is not None:
            client.stop(); self._health.mark_fail(key)

    async def close_all(self):
        clients = list(self._clients.values()); self._clients.clear()
        for client in clients:
            try: client.stop()
            except Exception: pass
        tasks = [c._task for c in clients if c._task is not None and not c._task.done()]
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)

async def h3_worker(pool: H3Pool, groups, args, stats, stop_event, scheduler, data_pool=None):
    if not HAS_AIOQUIC: _log("[ERROR] HTTP/3 需要 aioquic: pip install aioquic"); return
    rnd = random.Random(); idx = 0; h3_eps = []
    for g in groups:
        for ep in g.endpoints:
            h3_port = ep.h3_port if ep.h3_port else ep.port
            if ep.scheme == "https": h3_eps.append((ep, g, h3_port))
    if not h3_eps: _log("[WARN] 没有 h3 端点"); return
    n = len(h3_eps); base_extra = build_headers(args.header, args)

    while not stop_event.is_set():
        if scheduler: await scheduler.acquire()
        ep, group, h3_port = h3_eps[rnd.randrange(n)] if args.mode == "random" else h3_eps[idx % n]
        idx += 1

        data_ctx = data_pool.get_next() if data_pool else None
        path = group.build_path() if args.randomize else group.build_static_path()
        if data_ctx and args.format: path = render_template(args.format, data_ctx)

        hdrs = random_header_set() if args.randomize else {}
        for k, v in base_extra.items():
            if k.lower() not in ("user-agent", "accept-language", "cache-control", "pragma", "accept"):
                hdrs[k] = v

        req_headers = [(":method", group.method), (":path", path), (":scheme", "https"), (":authority", group.host_hdr)]
        for k, v in hdrs.items():
            if k.lower() not in ("host", "connection", "transfer-encoding", "content-length", "keep-alive"): req_headers.append((k, v))

        body_data = group.body
        if data_ctx and args.body: body_data = render_template(args.body, data_ctx).encode()
        if body_data: req_headers.append(("content-length", str(len(body_data))))

        start = time.perf_counter()
        try:
            client = await pool.get(ep, h3_port)
            status, resp_len = await client.request(req_headers, body_data)
            ms = (time.perf_counter() - start) * 1000.0
            ok = True if args.ignore_status else (status < 400 if status else False)
            stats.record(status=status, bytes_in=resp_len if not args.skip_body else 0, ms=ms, ok=ok,
                         endpoint=f"{ep.ip}:{h3_port} [h3]", circuit_breaker=args.circuit_breaker, stop_event=stop_event)
        except asyncio.CancelledError: raise
        except Exception as e:
            ms = (time.perf_counter() - start) * 1000.0
            stats.record(status=0, ms=ms, ok=False, err=type(e).__name__, endpoint=f"{ep.ip}:{h3_port} [h3]",
                         circuit_breaker=args.circuit_breaker, stop_event=stop_event)
            if is_conn_level_error(e):
                try: await pool.invalidate(ep, h3_port)
                except Exception: pass

# ===========================================================================
# [H6] WebSocket 引擎 (新旧版本参数兼容 + 禁用 deflate)
# ===========================================================================
def _ws_connect(ws_url, headers, timeout, ssl_ctx):
    """兼容 websockets 新旧版本的 connect 封装; 默认禁用 permessage-deflate 省 CPU"""
    def _mk(hdr_kw, with_compression=True):
        kwargs = {"open_timeout": timeout, "close_timeout": timeout}
        if with_compression: kwargs["compression"] = None
        if ssl_ctx is not None: kwargs["ssl"] = ssl_ctx
        if hdr_kw: kwargs[hdr_kw] = headers
        return websockets.connect(ws_url, **kwargs)
    for kw in ("additional_headers", "extra_headers"):
        try:
            return _mk(kw)
        except TypeError:
            continue
    try:
        return _mk(None)
    except TypeError:
        return _mk(None, with_compression=False)

async def ws_worker(groups, args, stats, stop_event, scheduler, data_pool=None):
    if not HAS_WEBSOCKETS:
        _log("[ERROR] WebSocket 引擎需要: pip install websockets")
        return

    rnd = random.Random(); idx = 0
    all_eps = [ep for g in groups for ep in g.endpoints]
    n = len(all_eps)
    if n == 0: return
    ep_group = build_ep_group_map(groups)
    base_extra = build_headers(args.header, args)

    while not stop_event.is_set():
        if scheduler: await scheduler.acquire()
        ep = all_eps[rnd.randrange(n)] if args.mode == "random" else all_eps[idx % n]
        idx += 1
        group = ep_group.get(id(ep), groups[0])

        data_ctx = data_pool.get_next() if data_pool else None
        path = group.build_static_path()
        if data_ctx and args.format: path = render_template(args.format, data_ctx)

        ws_url = f"ws://{ep.conn_host}:{ep.port}{path}"
        if ep.scheme == "https": ws_url = f"wss://{ep.conn_host}:{ep.port}{path}"

        start = time.perf_counter()
        try:
            ssl_ctx = None
            if ep.scheme == "https":
                ssl_ctx = ServerDiscovery.make_ssl_ctx(ep.hostname, args.insecure, None)

            async with _ws_connect(ws_url, base_extra, args.timeout, ssl_ctx) as websocket:
                msg = "ping"
                if data_ctx and args.body:
                    msg = render_template(args.body, data_ctx)
                elif args.body:
                    msg = args.body.decode() if isinstance(args.body, bytes) else args.body

                await websocket.send(msg)
                resp = await asyncio.wait_for(websocket.recv(), timeout=args.timeout)
                resp_len = len(resp) if isinstance(resp, (bytes, str)) else 0

                ms = (time.perf_counter() - start) * 1000.0
                stats.record(status=101, bytes_in=resp_len, bytes_out=len(msg), ms=ms, ok=True,
                             endpoint=ep.display, circuit_breaker=args.circuit_breaker, stop_event=stop_event)
        except asyncio.CancelledError: raise
        except Exception as e:
            ms = (time.perf_counter() - start) * 1000.0
            stats.record(status=0, ms=ms, ok=False, err=type(e).__name__, endpoint=ep.display,
                         circuit_breaker=args.circuit_breaker, stop_event=stop_event)

# ===========================================================================
# aiohttp 引擎
# ===========================================================================
async def aiohttp_worker(groups, args, session, stats, stop_event, scheduler, data_pool=None):
    rnd = random.Random(); idx = 0
    all_eps = [ep for g in groups for ep in g.endpoints]
    n = len(all_eps)
    if n == 0: return
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    ep_group = build_ep_group_map(groups)
    base_extra = build_headers(args.header, args)

    hdr_variants = []
    for _ in range(64):
        if args.randomize:
            hdrs = random_header_set()
            for k, v in base_extra.items():
                if k.lower() not in ("user-agent", "accept-language", "cache-control", "pragma", "accept"): hdrs[k] = v
        else:
            hdrs = dict(base_extra)
        hdr_variants.append(hdrs)

    while not stop_event.is_set():
        if scheduler: await scheduler.acquire()
        ep = all_eps[rnd.randrange(n)] if args.mode == "random" else all_eps[idx % n]
        idx += 1
        group = ep_group.get(id(ep), groups[0])

        data_ctx = data_pool.get_next() if data_pool else None
        path = group.build_path() if args.randomize else group.build_static_path()
        if data_ctx and args.format: path = render_template(args.format, data_ctx)
        url = f"{ep.scheme}://{ep.conn_host}:{ep.port}{path}"
        hdrs = hdr_variants[rnd.randrange(len(hdr_variants))]

        body_data = group.body
        if data_ctx and args.body: body_data = render_template(args.body, data_ctx).encode()

        start = time.perf_counter()
        try:
            async with session.request(group.method, url, data=body_data, timeout=timeout,
                                       allow_redirects=False, auto_decompress=False, headers=hdrs) as resp:
                if args.skip_body:
                    await resp.release(); length = 0
                else:
                    length = len(await resp.read())
                ms = (time.perf_counter() - start) * 1000.0
                ok = True if args.ignore_status else resp.status < 400
                stats.record(status=resp.status, bytes_in=length, ms=ms, ok=ok, endpoint=ep.display,
                             circuit_breaker=args.circuit_breaker, stop_event=stop_event)
        except asyncio.CancelledError: raise
        except Exception as e:
            ms = (time.perf_counter() - start) * 1000.0
            stats.record(status=0, ms=ms, ok=False, err=type(e).__name__, endpoint=ep.display,
                         circuit_breaker=args.circuit_breaker, stop_event=stop_event)


# ===========================================================================
# 辅助函数
# ===========================================================================
def build_headers(header_list, args):
    headers = {"Accept": "*/*"}
    if not args.randomize: headers["User-Agent"] = f"termux_http_stress/{VERSION}"
    for item in header_list:
        if ":" not in item: continue
        key, value = item.split(":", 1)
        headers[key.strip()] = value.strip()
    return headers

def make_aiohttp_connector(args):
    if args.insecure:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False; ssl_ctx.verify_mode = ssl.CERT_NONE
    else: ssl_ctx = None
    return aiohttp.TCPConnector(
        limit=args.workers * 2, limit_per_host=args.workers, keepalive_timeout=args.keepalive_timeout,
        enable_cleanup_closed=True, force_close=False, ttl_dns_cache=300, use_dns_cache=True, ssl=ssl_ctx, read_bufsize=args.read_bufsize
    )

def parse_thresholds(threshold_str: str) -> list:
    thresholds = []
    if not threshold_str: return thresholds
    for part in threshold_str.split(';'):
        part = part.strip()
        if not part: continue
        for op in ['<=', '>=', '<', '>']:
            if op in part:
                key, val = part.split(op, 1)
                thresholds.append((key.strip(), op, val.strip()))
                break
    return thresholds

def check_thresholds(stats, thresholds: list, elapsed: float) -> bool:
    if not thresholds: return True
    for key, op, val in thresholds:
        try:
            if key.startswith("status_"):
                code = int(key.split("_")[1])
                actual = stats.status_codes.get(code, 0) / max(stats.requests, 1)
            elif key == "p50": actual = stats.p50
            elif key == "p95": actual = stats.p95
            elif key == "p99": actual = stats.p99
            elif key == "error_rate": actual = stats.error_rate
            elif key == "rps": actual = stats.requests / max(elapsed, 0.001)
            else: continue

            if val.endswith('ms'): target = float(val[:-2])
            elif val.endswith('s'): target = float(val[:-1]) * 1000
            elif val.endswith('%'): target = float(val[:-1]) / 100.0
            else: target = float(val)

            if op == "<" and not (actual < target): return False
            if op == "<=" and not (actual <= target): return False
            if op == ">" and not (actual > target): return False
            if op == ">=" and not (actual >= target): return False
        except Exception: pass
    return True

def generate_html_report(stats: Stats, elapsed: float, filename: str):
    rps_data = ", ".join(f"[{t:.1f},{v:.1f}]" for t, v in stats._timeline_rps)
    lat_data = ", ".join(f"[{t:.1f},{p50:.1f},{p99:.1f}]" for t, p50, p99 in stats._timeline_lat)
    status_labels = ", ".join(f'"{k}"' for k in stats.status_codes.keys())
    status_data = ", ".join(str(v) for v in stats.status_codes.values())

    html_content = f"""<!DOCTYPE html>
<html lang="zh"><head><meta charset="UTF-8"><title>Stress Test Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>body {{ font-family: 'Segoe UI', sans-serif; margin: 20px; background: #f8f9fa; }}
.container {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; max-width: 1400px; margin: auto; }}
.card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
h1, h2 {{ color: #333; }} .metric {{ font-size: 24px; font-weight: bold; color: #007bff; }}
.metric-label {{ font-size: 14px; color: #6c757d; }}
.summary {{ display: flex; flex-wrap: wrap; gap: 20px; justify-content: space-around; margin-bottom: 20px; }}
.summary-item {{ text-align: center; }} canvas {{ max-height: 300px; }}</style></head>
<body><h1 style="text-align: center;">压测报告</h1>
<div class="summary">
    <div class="summary-item"><div class="metric">{stats.requests:,}</div><div class="metric-label">Total Requests</div></div>
    <div class="summary-item"><div class="metric">{stats.success:,}</div><div class="metric-label">Success</div></div>
    <div class="summary-item"><div class="metric">{stats.failed:,}</div><div class="metric-label">Failed</div></div>
    <div class="summary-item"><div class="metric">{stats.requests/max(elapsed,0.001):.1f}</div><div class="metric-label">Avg RPS</div></div>
    <div class="summary-item"><div class="metric">{stats.p99:.1f}ms</div><div class="metric-label">P99 Latency</div></div>
    <div class="summary-item"><div class="metric">{stats.error_rate*100:.2f}%</div><div class="metric-label">Error Rate</div></div>
</div><div class="container">
    <div class="card"><h2>RPS & Latency Over Time</h2><canvas id="timeChart"></canvas></div>
    <div class="card"><h2>HTTP Status Codes</h2><canvas id="statusChart"></canvas></div>
</div><script>
new Chart(document.getElementById('timeChart'), {{
    type: 'line',
    data: {{ datasets: [
        {{ label: 'RPS', data: [{rps_data}], borderColor: 'rgba(54, 162, 235, 1)', yAxisID: 'y' }},
        {{ label: 'P99 (ms)', data: [{lat_data}], borderColor: 'rgba(255, 99, 132, 1)', yAxisID: 'y1' }}
    ]}},
    options: {{ scales: {{ y: {{ position: 'left', title: 'RPS' }}, y1: {{ position: 'right', title: 'Latency (ms)', grid: {{ drawOnChartArea: false }} }} }} }}
}});
new Chart(document.getElementById('statusChart'), {{
    type: 'pie',
    data: {{ labels: [{status_labels}], datasets: [{{ data: [{status_data}], backgroundColor: ['#36a2eb','#ff6384','#4bc0c0','#ff9f40','#9966ff','#ffcd56'] }}] }}
}});
</script></body></html>"""
    try:
        with open(filename, 'w', encoding='utf-8') as f: f.write(html_content)
        print(f"[INFO] HTML 报告已生成: {filename}")
    except Exception as e: print(f"[WARN] 生成 HTML 报告失败: {e}")

def final_report(stats: Stats, elapsed: float, groups):
    print("\n" + "=" * 60 + "\nSUMMARY\n" + "=" * 60)
    print(f"Duration         : {elapsed:.2f}s")
    print(f"Total requests   : {stats.requests}")
    if elapsed > 0: print(f"Avg RPS          : {stats.requests / elapsed:.2f}")
    print(f"Success          : {stats.success}\nFailed           : {stats.failed}\nError rate       : {stats.error_rate:.4f}")
    print(f"Bytes in         : {stats.bytes_in:,}\nBytes out        : {stats.bytes_out:,}")
    if stats.h2_pushes > 0:
        print(f"H2 Server Pushes : {stats.h2_pushes}")
    if stats.connect_count > 0:
        print(f"Avg connect time : {stats.connect_ms_sum / stats.connect_count:.2f}ms "
              f"(over {stats.connect_count} new conns)")  # [H7]
    if stats.requests > 0:
        print(f"Avg latency      : {stats.lat_sum / stats.requests:.2f}ms")
        if stats.lat_min is not None: print(f"Min latency      : {stats.lat_min:.2f}ms")
        print(f"Max latency      : {stats.lat_max:.2f}ms\nP50 latency      : {stats.p50:.2f}ms\nP95 latency      : {stats.p95:.2f}ms\nP99 latency      : {stats.p99:.2f}ms")
    if stats.status_codes:
        print("\nHTTP status codes:")
        for code in sorted(stats.status_codes.keys()): print(f"  {code}: {stats.status_codes[code]}")
    if stats.errors:
        print("\nErrors:")
        for err in sorted(stats.errors.keys()): print(f"  {err}: {stats.errors[err]}")
    if stats._per_endpoint:
        print("\nPer-endpoint requests:")
        for ep, count in sorted(stats._per_endpoint.items(), key=lambda x: -x[1]): print(f"  {ep}: {count}")
    print("=" * 60)

def print_cluster_summary(agg: ClusterAggregator, elapsed: float):
    t = agg.totals()
    print("\n" + "=" * 60 + "\nCLUSTER SUMMARY\n" + "=" * 60)
    print(f"Workers reporting: {len(agg.latest)}")
    print(f"Duration         : {elapsed:.2f}s")
    print(f"Total requests   : {t['req']}")
    if elapsed > 0: print(f"Avg RPS          : {t['req'] / elapsed:.2f}")
    print(f"Success          : {t['ok']}\nFailed           : {t['fail']}")
    err = t['fail'] / t['req'] if t['req'] else 0.0
    print(f"Error rate       : {err:.4f}")
    print(f"Bytes in         : {t['bytes_in']:,}\nBytes out        : {t['bytes_out']:,}")
    if t['h2_pushes']: print(f"H2 Server Pushes : {t['h2_pushes']}")
    if t['connect_count'] > 0:
        print(f"Avg connect time : {t['connect_ms_sum'] / t['connect_count']:.2f}ms "
              f"(over {t['connect_count']} new conns)")
    if t['count'] > 0:
        print(f"Avg latency      : {t['lat_sum']/t['count']:.2f}ms")
        if t['lat_min'] is not None: print(f"Min latency      : {t['lat_min']:.2f}ms")
        print(f"Max latency      : {t['lat_max']:.2f}ms")
        print(f"P50 latency      : {merged_percentile(t['hist'], t['count'], 50):.2f}ms")
        print(f"P95 latency      : {merged_percentile(t['hist'], t['count'], 95):.2f}ms")
        print(f"P99 latency      : {merged_percentile(t['hist'], t['count'], 99):.2f}ms")
    if t['status_codes']:
        print("\nHTTP status codes:")
        for code in sorted(t['status_codes']): print(f"  {code}: {t['status_codes'][code]}")
    print("=" * 60)

def raise_fd_limit():
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        want = min(hard, 1048576)
        if soft < want:
            try: resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard)); print(f"[INFO] RLIMIT_NOFILE: {soft} -> {want}")
            except (ValueError, OSError) as e: print(f"[WARN] Cannot raise RLIMIT_NOFILE ({e}); soft={soft}")
    except ImportError: print("[INFO] Manually set 'ulimit -n 1048576' if needed")

def setup_signal_handlers(stop_event):
    def _handler(signum, frame): stop_event.set()
    old_int = signal.signal(signal.SIGINT, _handler)
    old_term = signal.signal(signal.SIGTERM, _handler)
    return old_int, old_term

def restore_signal_handlers(old_int, old_term):
    try: signal.signal(signal.SIGINT, old_int)
    except (ValueError, OSError): pass
    try: signal.signal(signal.SIGTERM, old_term)
    except (ValueError, OSError): pass

# ===========================================================================
# 数据池
# ===========================================================================
class DataPool:
    def __init__(self, filepath: str):
        self.data = []
        self.idx = 0
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.data = [row for row in reader]
            print(f"[INFO] 加载 {len(self.data)} 条 CSV 数据记录")
        except Exception as e:
            print(f"[ERROR] 读取 CSV 文件失败: {e}")

    def get_next(self) -> Optional[dict]:
        if not self.data: return None
        item = self.data[self.idx % len(self.data)]
        self.idx += 1
        return item


# ===========================================================================
# 通用压测核心 (单进程 / 多进程 worker / 远程 worker 共用)
# ===========================================================================
async def run_load(args, norm_targets, body, engine, has_tcp, stop_event,
                   on_report=None, enable_export=False):
    data_pool = DataPool(args.data_file) if args.data_file else None

    prefer_ips = None
    if args.prefer_ips: prefer_ips = [ip.strip() for ip in args.prefer_ips.split(",") if ip.strip()]

    groups = []
    for target_url in norm_targets:
        if args.no_discovery:
            if "://" not in target_url: target_url = f"{args.default_scheme}://{target_url}"
            parsed = urlparse(target_url)
            hostname = parsed.hostname or target_url
            is_tls = (parsed.scheme == "https")
            port = parsed.port or (443 if is_tls else 80)
            scheme = "https" if is_tls else "http"
            base_path = parsed.path if parsed.path else (args.path if args.path.startswith("/") else "/" + args.path)
            if parsed.query: base_path += "?" + parsed.query
            host_hdr = hostname if (is_tls and port == 443) or (not is_tls and port == 80) else f"{hostname}:{port}"
            base_query = base_path.split("?", 1)[1] if "?" in base_path else ""
            group = TargetGroup(hostname=hostname, port=port, scheme=scheme, base_path=base_path,
                                base_query=base_query, method=args.method, body=body, host_hdr=host_hdr)
            ep = Endpoint(ip=hostname, port=port, hostname=hostname, scheme=scheme, is_v6=(':' in hostname), alpn="http/1.1")
            group.endpoints.append(ep); groups.append(group)
        else:
            group = await ServerDiscovery.discover(target_url, args.path, args.method, body, args.insecure, args.discovery_timeout, prefer_ips)
            groups.append(group)

    _log(f"\n[INFO] 发现完成: {len(groups)} 组目标, {sum(len(g.endpoints) for g in groups)} 个端点")

    if engine == "auto":
        has_h2 = any(ep.alpn == "h2" for g in groups for ep in g.endpoints)
        has_h3 = any(ep.h3_port > 0 for g in groups for ep in g.endpoints)
        if has_h3 and HAS_AIOQUIC: engine = "h3"
        elif has_h2 and HAS_H2: engine = "h2"
        elif HAS_AIOHTTP: engine = "aiohttp"
        else: engine = "raw"
        _log(f"[INFO] 自动选择引擎: {engine}")

    scheduler = None
    if args.rps > 0 or args.rate_mode == "arrival":
        scheduler = Scheduler(args.rps, mode=args.rate_mode, stop_event=stop_event, burst=args.rps_burst)
        _log(f"[INFO] 调度器: {args.rate_mode} mode, rate={args.rps}")

    ssl_ctx = None
    if any(g.scheme == "https" for g in groups):
        alpn = ["h2", "http/1.1"] if engine in ("h2", "auto") else None
        ssl_ctx = ServerDiscovery.make_ssl_ctx("", args.insecure, alpn)

    tcp_intf = TCPInterference(args)
    ack_injector = RawACKInjector() if args.tcp_ack_inject else None
    if ack_injector and not ack_injector.available: _log("[WARN] ACK 注入需要 CAP_NET_RAW, 已禁用")

    health = EndpointHealth(cooldown=args.ep_cooldown)
    stats = Stats()

    if enable_export and args.export:
        try:
            ext = args.export; filename = f"{args.export_file}.{ext}"
            stats.enable_detail_log(filename, fmt=ext)
            print(f"[INFO] 详情流式导出: {filename}")
        except OSError as e:
            print(f"[WARN] 无法打开导出文件 ({e}); 导出已禁用"); args.export = None

    h2_pool = H2Pool(groups, args, health, stats) if engine == "h2" else None  # [H7] 传入 stats
    h3_pool = H3Pool(args, health) if engine == "h3" else None

    workers = []; session = None; connector = None

    if engine == "aiohttp":
        connector = make_aiohttp_connector(args)
        session = aiohttp.ClientSession(connector=connector, auto_decompress=False)
        worker_func = aiohttp_worker
        worker_args = (groups, args, session, stats, stop_event, scheduler, data_pool)
    elif engine == "h2":
        worker_func = h2_worker
        worker_args = (h2_pool, groups, args, stats, stop_event, scheduler, data_pool)
    elif engine == "h3":
        worker_func = h3_worker
        worker_args = (h3_pool, groups, args, stats, stop_event, scheduler, data_pool)
    elif engine == "ws":
        worker_func = ws_worker
        worker_args = (groups, args, stats, stop_event, scheduler, data_pool)
    else:
        worker_func = raw_worker
        worker_args = (groups, args, ssl_ctx, tcp_intf, ack_injector, health, stats, stop_event, scheduler, data_pool)

    if args.dry_run:
        _log("[INFO] Dry-run: 发送少量请求验证连通性...")
        workers.append(asyncio.create_task(worker_func(*worker_args)))
        await asyncio.sleep(2.0)
        stop_event.set()
        await asyncio.gather(*workers, return_exceptions=True)
        stats.close_detail()
        return stats, groups

    def make_worker(): return worker_func(*worker_args)

    initial_workers = args.workers
    if args.ramp_up > 0: initial_workers = max(1, args.workers // 10)

    for i in range(max(1, initial_workers)):
        workers.append(asyncio.create_task(supervised(f"{engine}-w{i}", make_worker, stop_event)))

    ramp_task = None
    if args.ramp_up > 0:
        async def ramp_up():
            interval = 2.0; elapsed = 0
            while elapsed < args.ramp_up and not stop_event.is_set():
                await asyncio.sleep(interval); elapsed += interval
                if stop_event.is_set(): break
                target = int(args.workers * (elapsed / args.ramp_up))
                add = max(0, target - len(workers))
                for j in range(add):
                    workers.append(asyncio.create_task(supervised(f"{engine}-r{len(workers)}", make_worker, stop_event)))
                _log(f"[RAMP-UP] {elapsed:.0f}s: {len(workers)} workers")
        ramp_task = asyncio.create_task(ramp_up())

    reporter_task = None
    if on_report is not None and args.interval > 0:
        async def _rep():
            while not stop_event.is_set():
                try: await asyncio.wait_for(stop_event.wait(), timeout=args.interval)
                except asyncio.TimeoutError: pass
                if stop_event.is_set(): break
                try: on_report(stats)
                except Exception: pass
        reporter_task = asyncio.create_task(_rep())

    try:
        # [H5] 预热阶段: 发真实请求焐热 TLS/JIT/路由缓存, 但不计入统计
        if args.warmup > 0:
            stats._recording = False
            _log(f"[INFO] 预热 {args.warmup}s (请求不计入统计)...")
            try: await asyncio.wait_for(stop_event.wait(), timeout=args.warmup)
            except asyncio.TimeoutError: pass
            if not stop_event.is_set():
                stats.reset()
                _log("[INFO] 预热完成, 正式统计开始")

        if args.duration > 0:
            try: await asyncio.wait_for(stop_event.wait(), timeout=args.duration)
            except asyncio.TimeoutError: pass
        else:
            await stop_event.wait()
    finally:
        stop_event.set()
        if ramp_task:
            ramp_task.cancel()
            try: await ramp_task
            except asyncio.CancelledError: pass
        if scheduler: scheduler.stop()
        _log("[INFO] 优雅退出: 等待在途请求完成 (最多 5s)...")
        if workers:
            done, pending = await asyncio.wait(workers, timeout=5.0)
            for w in pending: w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
        if reporter_task:
            reporter_task.cancel()
            try: await reporter_task
            except asyncio.CancelledError: pass
        if h2_pool is not None: await h2_pool.close_all()
        if h3_pool is not None: await h3_pool.close_all()
        if session: await session.close()
        if connector: await connector.close()
        if ack_injector: ack_injector.close()
        stats.close_detail()
    return stats, groups
# ===========================================================================
# 单进程模式
# ===========================================================================
async def _run_single_process(args, norm_targets, body, engine, has_tcp):
    gc.disable(); gc.set_threshold(50000, 10, 10)
    stop_event = asyncio.Event()
    old_int, old_term = setup_signal_handlers(stop_event)

    state = {"last_req": 0, "last_time": time.time(), "last_bytes": 0}

    def on_report(stats):
        now = time.time()
        elapsed = now - stats.start_time
        if not stats._recording:
            # [H5] 预热期显示
            print(f"[{int(elapsed):4d}s] warmup  req(untimed)={stats.warmup_requests}", flush=True)
            state.update(last_req=0, last_time=now, last_bytes=0)
            return
        rps = (stats.requests - state["last_req"]) / max(now - state["last_time"], 1e-6)
        bw = (stats.bytes_in - state["last_bytes"]) / max(now - state["last_time"], 1e-6)
        status_snap = ""
        if stats.status_codes:
            top = sorted(stats.status_codes.items(), key=lambda x: -x[1])[:5]
            status_snap = " ".join(f"{k}:{v}" for k, v in top)
        print(f"[{int(elapsed):4d}s] req={stats.requests} ok={stats.success} fail={stats.failed} "
              f"rps={rps:.1f} p50={stats.p50:.1f}ms p95={stats.p95:.1f}ms p99={stats.p99:.1f}ms "
              f"bw={bw/1024:.1f}KB/s status=[{status_snap}]", flush=True)
        stats.record_timeline(elapsed, rps, stats.p50, stats.p99)
        state.update(last_req=stats.requests, last_time=now, last_bytes=stats.bytes_in)

    try:
        stats, groups = await run_load(args, norm_targets, body, engine, has_tcp, stop_event,
                                       on_report=on_report, enable_export=True)
        elapsed = time.time() - stats.start_time
        final_report(stats, elapsed, groups)

        if args.report_html: generate_html_report(stats, elapsed, args.report_html)

        exit_code = 0
        if args.threshold:
            thresholds = parse_thresholds(args.threshold)
            if thresholds:
                if check_thresholds(stats, thresholds, elapsed):
                    print("[INFO] 阈值断言: PASSED")
                else:
                    print("[ERROR] 阈值断言: FAILED"); exit_code = 2
        if args.export:
            print(f"\n[EXPORT] 结果已导出: {args.export_file}.{args.export}")
        return exit_code
    finally:
        gc.enable()
        restore_signal_handlers(old_int, old_term)


# ===========================================================================
# 本地多进程 Worker
# ===========================================================================
def worker_process_main(args_dict, norm_targets, body, engine, has_tcp, worker_id, stats_q, mp_stop):
    global VERBOSE
    VERBOSE = (worker_id == 0)  # 只让 0 号进程打印发现日志, 避免刷屏

    gc.disable(); gc.set_threshold(50000, 10, 10)
    args = argparse.Namespace(**args_dict)

    # RPS 分片: 每个子进程平分总速率
    if args.processes > 1 and args.rps > 0:
        args.rps = args.rps / args.processes

    if args.uvloop and HAS_UVLOOP:
        try: asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except Exception: pass

    try:
        asyncio.run(worker_run_async(args, norm_targets, body, engine, has_tcp, worker_id, stats_q, mp_stop))
    except KeyboardInterrupt:
        pass
    finally:
        gc.enable()


async def worker_run_async(args, norm_targets, body, engine, has_tcp, worker_id, stats_q, mp_stop):
    stop_event = asyncio.Event()
    old_int, old_term = setup_signal_handlers(stop_event)

    # multiprocessing.Event 无异步回调; 用轮询代理转发停止信号
    async def _stop_proxy():
        while not mp_stop.is_set():
            await asyncio.sleep(0.2)
        stop_event.set()
    proxy_task = asyncio.create_task(_stop_proxy())

    def on_report(stats):
        try: stats_q.put(stats.snapshot(wid=worker_id))
        except Exception: pass

    try:
        stats, groups = await run_load(args, norm_targets, body, engine, has_tcp, stop_event, on_report=on_report)
        try: stats_q.put(stats.snapshot(wid=worker_id, final=True))
        except Exception: pass
    finally:
        proxy_task.cancel()
        restore_signal_handlers(old_int, old_term)

# ===========================================================================
# 本地多进程 Master
# ===========================================================================
def master_loop(args, norm_targets, body, engine, has_tcp):
    mp_stop = multiprocessing.Event()
    stats_q = multiprocessing.Queue()
    args_dict = vars(args)

    agg = ClusterAggregator()  # [H8] 计时起点与 spawn 对齐
    procs = []
    for i in range(args.processes):
        procs.append(multiprocessing.Process(
            target=worker_process_main,
            args=(args_dict, norm_targets, body, engine, has_tcp, i, stats_q, mp_stop)))
    for pr in procs: pr.start()

    start = agg.start_time
    last_print = start; last_req = 0
    # [H8] 子进程实际运行 = warmup + duration, Master 的停止时限必须计入 warmup
    total_run = args.duration + args.warmup if args.duration > 0 else None
    hard_deadline = (start + total_run + 120) if total_run else None
    exit_code = 0

    try:
        while True:
            try:
                while True:
                    snap = stats_q.get(timeout=0.2)
                    agg.apply(snap)
            except queue_mod.Empty:
                pass

            now = time.time()
            if now - last_print >= args.interval:
                t = agg.totals()
                rps = (t["req"] - last_req) / max(now - last_print, 1e-6)
                print(f"[Master {int(now-start):4d}s] procs={args.processes} req={t['req']} ok={t['ok']} "
                      f"fail={t['fail']} rps={rps:.1f} "
                      f"p99={merged_percentile(t['hist'], t['count'], 99):.1f}ms", flush=True)
                last_print = now; last_req = t["req"]

            if total_run and now - start >= total_run:
                mp_stop.set()

            if all(not pr.is_alive() for pr in procs):
                break
            if hard_deadline and now > hard_deadline:
                break
    except KeyboardInterrupt:
        print("\n[Master] Interrupted; stopping workers...")
    finally:
        mp_stop.set()
        for pr in procs:
            pr.join(timeout=5.0)
            if pr.is_alive(): pr.terminate()
        try:
            while True:
                agg.apply(stats_q.get_nowait())
        except queue_mod.Empty:
            pass

    elapsed = time.time() - agg.start_time
    print_cluster_summary(agg, elapsed)

    if args.threshold:
        view = MergedStatsView(agg.totals())
        if check_thresholds(view, parse_thresholds(args.threshold), elapsed):
            print("[INFO] 阈值断言: PASSED")
        else:
            print("[ERROR] 阈值断言: FAILED"); exit_code = 2
    return exit_code


# ===========================================================================
# 远程分布式: Master (--serve) 与 Worker (--master-host)
# ===========================================================================
async def remote_master_main(args):
    agg = ClusterAggregator()
    stop = asyncio.Event()
    old_int, old_term = setup_signal_handlers(stop)

    async def _handle(reader, writer):
        peer = writer.get_extra_info("peername")
        wid = None
        try:
            while True:
                # 换行分帧: 绝不会 json.loads 到半行
                line = await reader.readline()
                if not line: break
                line = line.strip()
                if not line: continue
                try: msg = json.loads(line)
                except Exception: continue
                if msg.get("type") == "register":
                    wid = msg.get("wid") or str(peer)
                    agg.register(wid)
                    print(f"[Master] Worker 注册: {wid} ({peer}) coroutines={msg.get('coroutines', '?')}")
                    continue
                msg["wid"] = wid if wid is not None else str(peer)
                agg.apply(msg)
        except Exception:
            pass
        finally:
            try: writer.close()
            except Exception: pass

    server = await asyncio.start_server(_handle, "0.0.0.0", args.master_port)
    print(f"[Master] 监听 0.0.0.0:{args.master_port}, 等待 Worker 注册 (Ctrl+C 结束收集)...")
    start = time.time(); last_print = start; last_req = 0
    # 宽裕收尾: worker 自行 warmup+duration 后上报 final
    grace = args.duration + 120 if args.duration > 0 else None
    exit_code = 0
    try:
        while not stop.is_set():
            await asyncio.sleep(0.5)
            now = time.time()
            if now - last_print >= args.interval:
                t = agg.totals()
                rps = (t["req"] - last_req) / max(now - last_print, 1e-6)
                print(f"[Master {int(now-start):4d}s] workers={len(agg.latest)} req={t['req']} ok={t['ok']} "
                      f"fail={t['fail']} rps={rps:.1f} "
                      f"p99={merged_percentile(t['hist'], t['count'], 99):.1f}ms", flush=True)
                last_print = now; last_req = t["req"]
            if grace and now - start >= grace:
                print("[Master] 时限已到 (含收尾宽限), 停止收集")
                break
            if agg.all_done:
                print("[Master] 所有已注册 Worker 均已完成")
                await asyncio.sleep(1.0)
                break
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        restore_signal_handlers(old_int, old_term)

    elapsed = time.time() - agg.start_time
    print_cluster_summary(agg, elapsed)
    if args.threshold:
        view = MergedStatsView(agg.totals())
        if check_thresholds(view, parse_thresholds(args.threshold), elapsed):
            print("[INFO] 阈值断言: PASSED")
        else:
            print("[ERROR] 阈值断言: FAILED"); exit_code = 2
    return exit_code


async def remote_worker_main(args, norm_targets, body, engine, has_tcp):
    wid = f"{socket.gethostname()}#{os.getpid()}"
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(args.master_host, args.master_port), timeout=10.0)
    except Exception as e:
        print(f"[Worker] 无法连接 Master {args.master_host}:{args.master_port}: {e}")
        return

    print(f"[Worker] 已连接 Master, id={wid}")

    stop_event = asyncio.Event()
    old_int, old_term = setup_signal_handlers(stop_event)

    def on_report(stats):
        try:
            writer.write(json.dumps(stats.snapshot(wid=wid)).encode() + b"\n")
        except Exception:
            pass

    async def _drain_loop():
        while not stop_event.is_set():
            try: await writer.drain()
            except Exception: return
            await asyncio.sleep(1.0)
    drain_task = asyncio.create_task(_drain_loop())

    try:
        reg = {"type": "register", "wid": wid, "coroutines": args.workers}
        writer.write(json.dumps(reg).encode() + b"\n")
        await writer.drain()

        stats, groups = await run_load(args, norm_targets, body, engine, has_tcp, stop_event, on_report=on_report)

        final = stats.snapshot(wid=wid, final=True)
        writer.write(json.dumps(final).encode() + b"\n")
        await writer.drain()
        print(f"[Worker] 压测完成, 最终统计已上报 Master (req={stats.requests})")
    finally:
        drain_task.cancel()
        try: writer.close()
        except Exception: pass
        restore_signal_handlers(old_int, old_term)


# ===========================================================================
# Main
# ===========================================================================
def main():
    p = argparse.ArgumentParser(
        description=f"Termux HTTP Stress Test v{VERSION} — Ultimate Edition\n"
                    "Single/Multi-Process/Distributed + HTTP/1.1/2/3 + WS")

    p.add_argument("--version", action="version", version=f"stress_test v{VERSION}")
    p.add_argument("--targets", help="逗号分隔目标列表")
    p.add_argument("--file", help="目标文件, 每行一个")
    p.add_argument("--config", help="YAML 场景配置文件")
    p.add_argument("--path", default="/", help="默认路径")
    p.add_argument("--default-scheme", default="http")
    p.add_argument("--prefer-ips", help="手动指定后端 IP")

    p.add_argument("--method", default="GET")
    p.add_argument("--body", default=None)
    p.add_argument("--body-json", default=None)
    p.add_argument("--header", action="append", default=[])

    p.add_argument("--data-file", help="CSV 数据文件路径")
    p.add_argument("--format", help="URL 模板, 如 /api?user_id=${id}")

    # 分布式参数
    p.add_argument("--processes", type=int, default=1, help="本地多进程 Worker 数量 (RPS 自动均分)")
    p.add_argument("--serve", action="store_true", help="作为远程 Master 启动 (监听 --master-port)")
    p.add_argument("--master-port", type=int, default=8765, help="远程 Master 监听端口")
    p.add_argument("--master-host", type=str, default=None,
                   help="连接到远程 Master 的地址 (本进程作为远程 Worker 运行; 注意 rps 为本机份额)")

    p.add_argument("-w", "--workers", type=int, default=50, help="每个进程/Worker 的协程数")
    p.add_argument("-d", "--duration", type=float, default=30.0)
    p.add_argument("--warmup", type=float, default=0,
                   help="[v22] 预热秒数: 发真实请求焐热连接但不计入统计")  # [H5]
    p.add_argument("--timeout", type=float, default=10.0)
    p.add_argument("--mode", choices=["rr", "random"], default="rr")
    p.add_argument("--engine", choices=["auto", "aiohttp", "raw", "h2", "h3", "ws"], default="auto")
    p.add_argument("--max-conns-per-ep", type=int, default=64)
    p.add_argument("--h2-conns-per-ep", type=int, default=4)
    p.add_argument("--ep-cooldown", type=float, default=2.0)

    p.add_argument("--rps", type=float, default=0)
    p.add_argument("--rate-mode", choices=["worker", "arrival"], default="worker")
    p.add_argument("--rps-burst", type=float, default=None)
    p.add_argument("--ramp-up", type=float, default=0)

    p.add_argument("--threshold", type=str, default=None,
                   help="阈值断言 (p99<500ms;error_rate<0.01;status_500<10), 违约 exit 2")
    p.add_argument("--circuit-breaker", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--report-html", type=str, default=None)

    p.add_argument("--randomize", action="store_true", default=True)
    p.add_argument("--no-randomize", dest="randomize", action="store_false")
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--uvloop", action="store_true")

    p.add_argument("--ignore-status", action="store_true")
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--skip-body", action="store_true")

    p.add_argument("--keepalive-timeout", type=float, default=60.0)
    p.add_argument("--read-bufsize", type=int, default=65536)

    p.add_argument("--tcp-sndbuf", type=int, default=None)
    p.add_argument("--tcp-rcvbuf", type=int, default=None)
    p.add_argument("--tcp-window-clamp", type=int, default=None)
    p.add_argument("--tcp-nagle", action="store_true")
    p.add_argument("--tcp-linger-rst", action="store_true")
    p.add_argument("--tcp-churn", type=int, default=0,
                   help="每 N 请求清空连接池强制重连 (0=永不, v22 修正默认值)")
    p.add_argument("--tcp-small-segs", action="store_true",
                   help="以 1~16 字节小分段发送请求 (干扰测试)")
    p.add_argument("--tcp-ack-inject", action="store_true")

    p.add_argument("--export", choices=["json", "csv"], default=None)
    p.add_argument("--export-file", default="stress_results")
    p.add_argument("--no-discovery", action="store_true")
    p.add_argument("--discovery-timeout", type=float, default=5.0)
    p.add_argument("--i-have-authorization", action="store_true",
                   help="确认你已获得对目标的授权")

    args = p.parse_args()

    if args.config:
        cfg = load_yaml_config(args.config)
        if 'targets' in cfg: args.targets = cfg['targets']
        if 'path' in cfg: args.path = cfg['path']
        if 'method' in cfg: args.method = cfg['method']
        if 'body' in cfg: args.body = cfg['body']
        if 'headers' in cfg: args.header = [f"{k}:{v}" for k, v in cfg['headers'].items()]
        if 'workers' in cfg: args.workers = cfg['workers']
        if 'duration' in cfg: args.duration = cfg['duration']
        if 'warmup' in cfg: args.warmup = cfg['warmup']
        if 'rps' in cfg: args.rps = cfg['rps']
        if 'threshold' in cfg: args.threshold = cfg['threshold']
        if 'data_file' in cfg: args.data_file = cfg['data_file']
        if 'format' in cfg: args.format = cfg['format']

    if not args.i_have_authorization:
        print(AUTHORIZATION_BANNER)
        print("[ERROR] Use --i-have-authorization to confirm you have permission to test.")
        sys.exit(1)

    if args.uvloop:
        if HAS_UVLOOP:
            try: asyncio.set_event_loop_policy(uvloop.EventLoopPolicy()); print("[INFO] uvloop 已启用")
            except Exception as e: print(f"[WARN] uvloop 初始化失败 ({e}), 回退至默认 asyncio")
        else: print("[WARN] uvloop 不可用")

    body = None
    if args.body is not None: body = args.body.encode()
    elif args.body_json is not None: body = json_dumps(args.body_json)

    raw_targets = []
    if args.file:
        try:
            with open(args.file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"): continue
                    for part in line.split(","):
                        part = part.strip()
                        if part: raw_targets.append(part)
        except FileNotFoundError: print(f"[ERROR] 文件不存在: {args.file}"); sys.exit(1)
    elif args.targets: raw_targets = [t.strip() for t in args.targets.split(",") if t.strip()]
    elif os.getenv("TARGETS"): raw_targets = [t.strip() for t in os.getenv("TARGETS").split(",") if t.strip()]
    else: print("[ERROR] 未配置目标"); sys.exit(1)

    norm_targets = []
    for entry in raw_targets:
        entry = entry.strip().strip('"').strip("'")
        if not entry or entry.startswith("#"): continue
        if "://" not in entry: entry = f"{args.default_scheme}://{entry}"
        norm_targets.append(entry)
    if not norm_targets: print("[ERROR] 无有效目标"); sys.exit(1)

    engine = args.engine
    has_tcp = any([args.tcp_sndbuf is not None, args.tcp_rcvbuf is not None, args.tcp_window_clamp is not None,
                   args.tcp_nagle, args.tcp_linger_rst, args.tcp_churn > 0, args.tcp_small_segs, args.tcp_ack_inject])
    if has_tcp and engine in ("auto", "aiohttp", "h2", "h3", "ws"):
        print("[WARN] TCP 干扰选项需要 --engine raw; 切换到 raw")
        engine = "raw"
    if engine == "h2" and not HAS_H2: print("[ERROR] h2 引擎需要: pip install h2"); engine = "aiohttp" if HAS_AIOHTTP else "raw"
    if engine == "h3" and not HAS_AIOQUIC: print("[ERROR] h3 引擎需要: pip install aioquic"); engine = "aiohttp" if HAS_AIOHTTP else "raw"
    if engine == "ws" and not HAS_WEBSOCKETS: print("[ERROR] ws 引擎需要: pip install websockets"); engine = "raw"
    if engine == "aiohttp" and not HAS_AIOHTTP: print("[ERROR] aiohttp 引擎需要: pip install aiohttp"); engine = "raw"

    raise_fd_limit()

    print("=" * 70 + f"\nTERMUX HTTP STRESS TEST v{VERSION} (Ultimate Edition)\n" + "=" * 70)
    print(f"Engine           : {engine}")
    if args.serve:
        print(f"Mode             : Remote Master (port {args.master_port})")
    elif args.master_host:
        print(f"Mode             : Remote Worker -> {args.master_host}:{args.master_port}")
    elif args.processes > 1:
        print(f"Mode             : Local Multi-Process ({args.processes} procs)")
    else:
        print(f"Mode             : Single Process")
    print(f"Event loop       : {'uvloop' if (args.uvloop and HAS_UVLOOP) else 'asyncio'}"
          + (" + asyncio.timeout(C)" if _TIMEOUT_CTX is not None else ""))
    print(f"Targets          : {len(norm_targets)}")
    print(f"Workers per proc : {args.workers}")
    print(f"Duration         : {args.duration}s")
    print(f"Warmup           : {args.warmup}s" if args.warmup > 0 else "Warmup           : off")
    print(f"Timeout          : {args.timeout}s")
    print(f"Method           : {args.method}")
    print(f"Mode             : {args.mode}")
    print(f"Randomize        : {args.randomize}")
    print(f"RPS limit        : {args.rps if args.rps > 0 else 'unlimited'}")
    print(f"Rate mode        : {args.rate_mode}")
    print(f"Data file        : {args.data_file or 'off'}")
    print(f"Ramp-up          : {args.ramp_up}s" if args.ramp_up > 0 else "Ramp-up          : off")
    print(f"Ignore status    : {args.ignore_status}")
    print(f"Skip body        : {args.skip_body}")
    print(f"TLS insecure     : {args.insecure}")
    print(f"EP cooldown      : {args.ep_cooldown}s")
    print(f"Circuit breaker  : {args.circuit_breaker if args.circuit_breaker > 0 else 'off'}")
    if args.threshold: print(f"Thresholds       : {args.threshold}")
    if engine in ("h2", "auto"): print(f"H2 conns per ep  : {args.h2_conns_per_ep}")
    if has_tcp:
        print("-" * 70 + "\nTCP 干扰:")
        if args.tcp_sndbuf is not None: print(f"  SO_SNDBUF        : {args.tcp_sndbuf}")
        if args.tcp_rcvbuf is not None: print(f"  SO_RCVBUF        : {args.tcp_rcvbuf}")
        if args.tcp_window_clamp is not None: print(f"  TCP_WINDOW_CLAMP : {args.tcp_window_clamp}")
        print(f"  Nagle (禁NODELAY): {args.tcp_nagle}")
        print(f"  SO_LINGER RST    : {args.tcp_linger_rst}")
        print(f"  Churn interval   : {args.tcp_churn if args.tcp_churn > 0 else 'off (v22 默认修复)'}")
        print(f"  Small segments   : {args.tcp_small_segs}")
        print(f"  ACK injection    : {args.tcp_ack_inject}")
    print("=" * 70)

    # 路由到不同执行模式
    if args.serve:
        sys.exit(asyncio.run(remote_master_main(args)))

    if args.master_host:
        asyncio.run(remote_worker_main(args, norm_targets, body, engine, has_tcp))
        return

    if args.processes > 1:
        if sys.platform != 'win32':
            multiprocessing.set_start_method('fork', force=True)
        sys.exit(master_loop(args, norm_targets, body, engine, has_tcp))

    sys.exit(asyncio.run(_run_single_process(args, norm_targets, body, engine, has_tcp)))


if __name__ == "__main__":
    main()