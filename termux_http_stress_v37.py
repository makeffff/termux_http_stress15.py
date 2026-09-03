#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Termux HTTP Stress Test v36 — Universal Engine Edition

仅用于对已获授权的目标进行性能测试。

v28 修复 (对照 v27 两份评估报告, 共 6 项):
  FIX2 [稳定性/信号] 信号处理修复: 原 hasattr(asyncio, "add_signal_handler") 检查对象写错恒为
                    False, loop 主路径从未注册(死代码); 现正确检查 loop 并激活主路径, 二次 Ctrl+C
                    计数式强退 os._exit(130); signal 模块回退路径行为保持不变
  FIX3 [稳定性/集群] 心跳超时节点剔除: ClusterAggregator 新增 timed_out 集合与 drop_worker(),
                    watchdog 超时改调 drop_worker, 失联节点非 final 快照被清除, all_done 不再
                    永假(原 Master 会挂到 grace 或无限挂起); 汇总报告输出 timed_out 节点告警
  FIX4 [一致性/Rust] Rust 引擎状态码判定对齐 Python 四引擎: (200..500).contains(&resp.status)
                    改为 resp.status < 400, 400-499 计失败, 双引擎结果可比
  FIX5 [健壮性/Rust] 显式 --engine rust 且目标含 https:// 时硬校验报错退出(2): Rust core 无
                    TLS 栈, 不再静默失败; auto 模式既有排除逻辑不变
  FIX6 [稳定性/H2]   H2Pool.get() 并发建连防超限: 锁外建连后按 h2_conns_per_ep 条件追加, 超限
                    关闭多余新建连接并复用空闲连接, 池满沿用 busy 快速失败语义
  FIX7 [协议/集群]   注册握手回执: Master 认证成功即回 {"type": "registered"}; Worker 发送
                    register 后等待回执 15s, 非 registered (auth_failed/断连/超时) 立即中止
                    不再空跑; 注意与旧版不兼容, 集群节点需同版本配对部署

v27 修复 (对照 v26 全量审计, 共 2 项):
  R1 [稳定性/熔断] 熔断冷却期请求特判 swallowed: raw/h2/h3 worker 收到 "cooling down"
                    背压不再计入 failed/error_rate (防污染熔断反馈环), 短退避后 continue 存活
  S1 [稳定性/WS]   WebSocketPool 借用计数并发上限: 原 max_per_ep 只看空闲池长度, 并发借用
                    无上限导致连接风暴; 现按借用数严格约束, 池满抛 busy 由 worker 特判
                    swallowed+退避; 失效/归还连接统一 close, 消除 fd 泄漏 (get/put/evict 异步化)

v26 修复 (对照 v25 全量审计, 共 6 项):
  W1 [准确性/直方图] HDR 十进制直方图编码失真修复: 原 encode 将 [2*10^order, 10^(order+1))
                    区间整体钳入末桶, 高延迟段 P50/P99 严重偏低; 现改为每个 order 完整线性覆盖
  W2 [语义/dry-run]  --dry-run 或 --duration<=0(无限) 时禁止 Rust 引擎: Rust core 无法表达
                    单发探活与无限运行, auto 判定排除, 不再静默跑满负载
  W3 [健壮性]       直方图 _decode 改为浮点下界: 修复整数除法(order=1 时 width=0)导致的子桶精度丢失;
                    与 _encode 互为逆映射, 合并分位(集群/多进程)不失真
  W4 [可观测性]     直方图桶边界与容量文档化, order0 边界语义 (1us..10us 单 us 桶)
  W5 [稳定性/熔断]  EndpointHealth 探测超时兜底: HALF_OPEN 单飞探测若挂起超过恢复窗口,
                    自动放行下一次恢复探测, 避免端点因丢失探测结果而永久不可用
  W6 [可观测性]     auto 引擎与 randomize 语义冲突显式告警, 避免静默忽略随机化请求头

v25 修复 (对照 v24 全量审计, 共 6 项):
  V1 [稳定性/熔断]  EndpointHealth HALF_OPEN 单飞: 同一端点同一时刻只放行一个恢复探测, 杜绝探测风暴
  V2 [稳定性/停止]  h2/h3/ws/aiohttp 引擎补齐 scheduler 返回值检查 (与 raw 一致):
                    OneShotScheduler(dry-run) 或 stop 后 acquire 返回 False 立即退出 worker, 防止 dry-run/结束时空转
  V3 [稳定性/多进程] master_loop 注册修复: 按真实 worker id 注册聚合器, 修正 all_done/missing 判定
  V4 [准确性/预热]  Stats.reset 保留预热结束时在飞并发作为正式统计基线, 不再污染 peak_in_flight
  V5 [健壮性]       auto 引擎边界: Rust core 仅明文 HTTP/1.1 (无 TLS 栈), 目标含 https/h2/h3 时禁止自动选 rust, 避免静默失败
  V6 [健壮性]       EndpointHealth probing 状态在 mark_ok/mark_fail 时正确清理, 避免状态泄漏

v24 修复 (对照 v23 审计 14 项):
  F1 [L1/GIL]      多进程 fork 安全: 子进程内才创建事件循环资源; 每核独立调度
  F2 [L1/安全]     分布式 master/worker 认证 + 可插拔加密: HMAC token / TLS, 心跳+失联回收
  F3 [L1/背压]     arrival 队列丢弃纳入统计, --arrival-drop-policy drop|block|error
  F4 [L2/H2流控]   动态流控: 从 RemoteSettingsChanged 读真实 INITIAL_WINDOW_SIZE, 发送前精确取窗
  F5 [L2/直方图]   HDR 精确直方图 (64-bit sub-bucket, 支持 1μs..1024s), 线性插值 P50/P95/P99/P999
  F6 [L2/WS]       WebSocket 连接池: 复用连接、最大连接数、失效剔除
  F7 [L2/DataPool] 数据池无状态轮转 + 读失败快速失败 (fast-fail)
  F8 [L3/信号]     loop.add_signal_handler (Unix) + 二次 Ctrl+C 强制退出
  F9 [L3/熔断]     端点熔断器: CLOSED->OPEN->HALF_OPEN, 连续失败阈值 + 恢复探测
  F10 [L3/XSS]     HTML 报告全量 html.escape
  F11 [L4/Termux]  fd 上限: resource + shell ulimit 双路径, 预检并在启动摘要打印
  F12 [L4/日志]    --log-format text|json, 结构化日志
  F13 [L4/配置]    --show-effective-config 输出最终生效配置; YAML 覆盖规则文档化
  F14 [L4/降级]    引擎降级显式 WARN + 预期影响说明

v23 遗留修复:
  B1  H2 池建连移出 per-key 锁, 满流连接快速失败
  B2  H2 接收循环崩溃: 记录 root cause + 连接失效通知
  B3  raw 引擎 header 截断 -> 异常而非带脏数据归池
  B4  auto 引擎 h3 探测失败降级 h2
  B5  bytes_in 语义统一: 收到的 body 字节; 截断计数 truncated_responses
  B6  TTFB (首字节时间) 独立统计
  B7  in-flight 并发度统计
  B8  --export 采样率 --export-sample
  B9  is_conn_level_error 改用类继承判断 (保留名称匹配作兜底)
  B10 DataPool 失败退出
"""

import argparse
import asyncio
import base64
import csv
import gc
import hashlib
import hmac
import html as html_mod
import json
import multiprocessing
import os
import queue as queue_mod
import random
import re
import secrets
import signal
import shutil
import socket
import ssl
import struct
import sys
import time
import resource
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

# ============================================================================
# Embedded Rust data-plane core (single-file). Compiled on first use (cached).
# Zero external deps; LTO + panic=abort for max throughput.
# ============================================================================
RUST_CORE_SRC = r"""// ============================================================================
// TaichuAgentTeam stress-test  —  single-file data-plane core (Rust, std-only)
// ============================================================================
// Zero external dependencies. Compile directly:
//   rustc -O --edition=2021 termux_stress_core.rs -o stresscore
// (add -C lto -C panic=abort -C strip for max throughput)
//
// Fixes vs v23/v24-Python (see research/termux_http_stress23_缺点分析.md):
//   L1 exact TTFB  L2 precise histogram (decimal 100-sub-bucket, lin. interp.)
//   L3 no GC stalls (Rust, no GC)  L4 no fork bomb (threads, per-core)
//   F1-F12 + B1-B10 (see delivery notes for the full mapping)
// ============================================================================

mod breaker {
// Per-endpoint circuit breaker: CLOSED -> OPEN -> HALF_OPEN.
// Thread-safe via per-endpoint Mutex (cheap: only touched on failure/ready checks).

use std::time::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug)]
struct Ep {
    consecutive_fail: u32,
    state: State,
    opened_at: Option<Instant>,
    probe_in_flight: bool,
}

impl Default for Ep {
    fn default() -> Self {
        Self {
            consecutive_fail: 0,
            state: State::Closed,
            opened_at: None,
            probe_in_flight: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BreakerCfg {
    pub failure_threshold: u32, // 0 disables
    pub recovery: Duration,
}

#[derive(Debug, Clone)]
pub struct Breaker {
    map: Arc<Mutex<HashMap<u32, Ep>>>,
    cfg: Option<BreakerCfg>,
}

impl Breaker {
    pub fn new(cfg: BreakerCfg) -> Self {
        Self {
            map: Arc::default(),
            cfg: if cfg.failure_threshold > 0 {
                Some(cfg)
            } else {
                None
            },
        }
    }

    /// True if this endpoint may be used right now.
    pub fn ready(&self, id: u32) -> bool {
        let Some(cfg) = &self.cfg else {
            return true;
        };
        let mut g = self.map.lock().unwrap();
        let ep = g.entry(id).or_insert_with(Ep::default);
        match ep.state {
            State::Closed => true,
            State::Open => {
                if let Some(t) = ep.opened_at {
                    if t.elapsed() >= cfg.recovery {
                        ep.state = State::HalfOpen;
                        if ep.probe_in_flight {
                            false
                        } else {
                            ep.probe_in_flight = true;
                            true
                        }
                    } else {
                        false
                    }
                } else {
                    ep.state = State::HalfOpen;
                    ep.probe_in_flight = true;
                    true
                }
            }
            State::HalfOpen => {
                if ep.probe_in_flight { false } else { ep.probe_in_flight = true; true }
            }
        }
    }

    pub fn mark_ok(&self, id: u32) {
        let Some(cfg) = &self.cfg else {
            return;
        };
        let _ = cfg;
        let mut g = self.map.lock().unwrap();
        if let Some(ep) = g.get_mut(&id) {
            ep.consecutive_fail = 0;
            ep.probe_in_flight = false;
            if ep.state == State::HalfOpen {
                ep.state = State::Closed;
                ep.opened_at = None;
            }
        }
    }

    pub fn mark_fail(&self, id: u32) -> bool {
        // returns true if we just transitioned to OPEN
        let Some(cfg) = &self.cfg else {
            return false;
        };
        let mut g = self.map.lock().unwrap();
        let ep = g.entry(id).or_insert_with(Ep::default);
        ep.consecutive_fail += 1;
        if ep.state != State::Closed {
            // A failed half-open probe returns the endpoint to OPEN.
            if ep.state == State::HalfOpen {
                ep.state = State::Open;
                ep.opened_at = Some(Instant::now());
                ep.probe_in_flight = false;
            }
            return true;
        }
        if ep.consecutive_fail >= cfg.failure_threshold {
            ep.state = State::Open;
            ep.opened_at = Some(Instant::now());
            true
        } else {
            false
        }
    }

    /// Snapshot of (closed, open, half_open) counts.
    pub fn snapshot(&self) -> (usize, usize, usize) {
        let g = self.map.lock().unwrap();
        let mut c = (0usize, 0usize, 0usize);
        for ep in g.values() {
            match ep.state {
                State::Closed => c.0 += 1,
                State::Open => c.1 += 1,
                State::HalfOpen => c.2 += 1,
            }
        }
        c
    }
}
}

mod config {
// Runtime configuration. Populated by main.rs from CLI / control-plane handshake.

use std::time::Duration;

/// Re-export for convenience (Backpressure lives in rate.rs).
pub use super::rate::Backpressure;

#[derive(Debug, Clone)]
pub struct Config {
    pub targets: Vec<String>,       // host:port or scheme://host:port
    pub path: String,
    pub method: String,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>, // explicit extra headers (Host appended)
    pub workers: usize,
    pub duration: Duration,
    pub warmup: Duration,
    pub timeout: Duration,
    pub mode: TargetMode,
    pub rps: f64,                   // 0 = unlimited
    pub rate_mode: RateMode,
    pub rps_burst: f64,             // 0 = same as rps
    pub keepalive: bool,
    pub insecure: bool,
    pub scheme: Scheme,
    pub skip_body: bool,
    pub ignore_status: bool,
    // reporting
    pub report_interval: Duration,
    pub out: ReportOut,
    pub sample_every: u64,          // 1 = all
    pub csv_path: Option<String>,
    // circuit breaker (per-endpoint)
    pub ep_breaker: u32,            // consecutive fails to OPEN; 0 = disabled
    pub ep_recovery: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetMode {
    RoundRobin,
    Random,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateMode {
    Worker,   // per-worker token bucket
    Arrival,  // global arrival process (Poisson-ish)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    Http,
    Https,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportOut {
    Stdout,
    JsonPipe, // single JSON line at exit (control-plane handshake)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            targets: vec![],
            path: "/".into(),
            method: "GET".into(),
            body: Vec::new(),
            headers: Vec::new(),
            workers: 50,
            duration: Duration::from_secs(30),
            warmup: Duration::ZERO,
            timeout: Duration::from_secs(10),
            mode: TargetMode::RoundRobin,
            rps: 0.0,
            rate_mode: RateMode::Worker,
            rps_burst: 0.0,
            keepalive: true,
            insecure: false,
            scheme: Scheme::Http,
            skip_body: false,
            ignore_status: false,
            report_interval: Duration::from_secs(2),
            out: ReportOut::Stdout,
            sample_every: 1,
            csv_path: None,
            ep_breaker: 5,
            ep_recovery: Duration::from_secs(30),
        }
    }
}

/// Parse a duration given in seconds (allow fractional).
pub fn dur_secs(s: f64) -> Duration {
    Duration::from_secs_f64(s.max(0.0))
}
}

mod histogram {
// Precise latency histogram (decimal, 100 sub-buckets, linear interpolation).
// Zero-dependency. Records values in microseconds (u64) over 1us .. 1_000_000_000us
// (1 µs .. 1000 s). Percentiles use linear interpolation across bucket boundaries.

/// Number of sub-buckets per decimal order of magnitude.
const SUB: usize = 100;
/// Decimal magnitude ranges. Index 0 covers [1,10), index i covers [10^i, 10^(i+1)).
/// We support up to 1000 s = 10^9 µs, so we need orders 0..=9.
const ORDERS: usize = 10;
const MAX_BUCKETS: usize = ORDERS * SUB; // 1000

#[derive(Debug, Clone)]
pub struct Histogram {
    // bucket index -> count
    counts: [u64; MAX_BUCKETS],
    pub count: u64,
    pub min_us: u64,
    pub max_us: u64,
    pub sum_us: u128,
}

impl Histogram {
    pub fn new() -> Self {
        Self {
            min_us: u64::MAX,
            max_us: 0,
            counts: [0; MAX_BUCKETS],
            count: 0,
            sum_us: 0,
        }
    }

    /// Bucket for a value in microseconds (1-based).
    fn bucket_of(value_us: u64) -> usize {
        if value_us == 0 {
            return 0;
        }
        // number of digits minus 1
        let v = value_us as u128;
        let mut order = 0usize;
        let mut x = v;
        while x >= 10 {
            x /= 10;
            order += 1;
            if order >= ORDERS - 1 {
                break;
            }
        }
        // clamp order to last
        let order = order.min(ORDERS - 1);
        let base: u128 = 10u128.pow(order as u32);
        let next = base * 10;
        let frac = if next > base {
            ((v - base) * SUB as u128 / (next - base)).min(SUB as u128 - 1)
        } else {
            0
        };
        order * SUB + frac as usize
    }

    pub fn record(&mut self, value_us: u64) {
        let b = Self::bucket_of(value_us);
        self.counts[b] += 1;
        self.count += 1;
        if value_us < self.min_us {
            self.min_us = value_us;
        }
        if value_us > self.max_us {
            self.max_us = value_us;
        }
        self.sum_us += value_us as u128;
    }

    pub fn avg_us(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_us as f64 / self.count as f64
        }
    }

    /// Percentile (0..=100) via linear interpolation.
    pub fn percentile(&self, p: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        if p <= 0.0 {
            return self.min_us as f64;
        }
        if p >= 100.0 {
            return self.max_us as f64;
        }
        let target = p / 100.0 * self.count as f64;
        let mut cumulative: f64 = 0.0;
        for b in 0..MAX_BUCKETS {
            let c = self.counts[b] as f64;
            if c == 0.0 {
                continue;
            }
            if cumulative + c >= target {
                // interpolate within this bucket
                let bucket_min = Self::bucket_min_us(b);
                let bucket_max = Self::bucket_max_us(b);
                let frac_in_bucket = if c > 0.0 {
                    (target - cumulative) / c
                } else {
                    0.0
                };
                return bucket_min as f64 + (bucket_max as f64 - bucket_min as f64) * frac_in_bucket;
            }
            cumulative += c;
        }
        self.max_us as f64
    }

    fn bucket_min_us(b: usize) -> u64 {
        let order = b / SUB;
        let sub = b % SUB;
        let base: u128 = 10u128.pow(order as u32);
        let next = base * 10;
        let step = (next - base) / SUB as u128;
        (base + step * sub as u128) as u64
    }

    fn bucket_max_us(b: usize) -> u64 {
        let order = b / SUB;
        let sub = b % SUB;
        let base: u128 = 10u128.pow(order as u32);
        let next = base * 10;
        let step = (next - base) / SUB as u128;
        (base + step * (sub as u128 + 1)) as u64
    }
}

/// Merge another histogram into this one.
impl Histogram {
    pub fn merge(&mut self, other: &Histogram) {
        for b in 0..MAX_BUCKETS {
            self.counts[b] += other.counts[b];
        }
        self.count += other.count;
        if other.min_us < self.min_us {
            self.min_us = other.min_us;
        }
        if other.max_us > self.max_us {
            self.max_us = other.max_us;
        }
        self.sum_us += other.sum_us;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buckets_and_percentiles() {
        let mut h = Histogram::new();
        // record 1..=1000 µs
        for i in 1..=1000 {
            h.record(i as u64);
        }
        assert_eq!(h.count, 1000);
        assert_eq!(h.min_us, 1);
        assert_eq!(h.max_us, 1000);
        let p50 = h.percentile(50.0);
        let p99 = h.percentile(99.0);
        assert!(p50 > 400.0 && p50 < 600.0, "p50={p50}");
        assert!(p99 > 900.0 && p99 <= 1000.0, "p99={p99}");
    }

    #[test]
    fn test_merge() {
        let mut a = Histogram::new();
        let mut b = Histogram::new();
        for i in 1..=100 {
            a.record(i as u64);
        }
        for i in 101..=200 {
            b.record(i as u64);
        }
        a.merge(&b);
        assert_eq!(a.count, 200);
        let p99 = a.percentile(99.0);
        assert!(p99 > 180.0 && p99 <= 200.0, "p99={p99}");
    }
}
}

mod http1 {
// HTTP/1.1 client over raw TCP. Zero external deps. Parses status + headers,
// reads body exactly per Content-Length / chunked, measures TTFB precisely,
// reuses keep-alive connections, classifies errors.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Instant;

use super::config::Scheme;

#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub host: String,      // Host header value (with port if non-default)
    pub authority: (String, u16), // (host, port) to connect
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub scheme: Scheme,
}

pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub ttfb_us: u64,
    pub total_us: u64,
    pub truncated: bool,
    pub keepalive: bool,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

/// Resolve + connect with a bounded connect timeout (non-blocking connect + poll).
/// Zero external deps; uses libc FFI (std already links libc on Unix).
pub fn connect(
    host: &str,
    port: u16,
    timeout: std::time::Duration,
) -> Result<TcpStream, String> {
    let start = Instant::now();
    let addrs: Vec<_> = (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("resolve: {e}"))?
        .collect();
    if addrs.is_empty() {
        return Err("resolve: no addresses".into());
    }
    let mut last_err = String::from("no addr");
    let deadline = Instant::now() + timeout;
    for a in &addrs {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match connect_bounded(*a, remaining) {
            Ok(s) => {
                let _ = s.set_read_timeout(Some(timeout));
                let _ = s.set_write_timeout(Some(timeout));
                let _ = s.set_nodelay(true);
                return Ok(s);
            }
            Err(e) => last_err = format!("connect {a}: {e}"),
        }
    }
    let _ = start;
    Err(last_err)
}



pub fn connect_us(host: &str, port: u16, timeout: std::time::Duration) -> (Option<TcpStream>, u64) {
    let start = Instant::now();
    match connect(host, port, timeout) {
        Ok(s) => (Some(s), start.elapsed().as_micros() as u64),
        Err(_) => (None, start.elapsed().as_micros() as u64),
    }
}

/// Connect with a bounded connect timeout (non-blocking connect + poll).
/// Zero external deps; libc FFI (std links libc on Unix).
#[cfg(unix)]
fn connect_bounded(addr: std::net::SocketAddr, timeout: std::time::Duration) -> Result<TcpStream, String> {
    use std::os::unix::io::{AsRawFd, FromRawFd};
    extern "C" {
        fn socket(domain: i32, ty: i32, protocol: i32) -> i32;
        fn fcntl(fd: i32, cmd: i32, arg: i32) -> i32;
        fn connect(fd: i32, a: *const u8, l: u32) -> i32;
        fn poll(f: *mut PollFd, n: u64, t: i32) -> i32;
        fn getsockopt(fd: i32, l: i32, o: i32, v: *mut i8, len: *mut u32) -> i32;
        fn close(fd: i32) -> i32;
        fn __errno_location() -> *mut i32;
    }
    #[repr(C)]
    #[derive(Default)]
    struct PollFd { fd: i32, ev: u16, re: u16 }
    const AF_INET: i32 = 2;
    const AF_INET6: i32 = 10;
    const SOCK_STREAM: i32 = 1;
    const O_NONBLOCK: i32 = 2048;
    const F_GETFL: i32 = 3;
    const F_SETFL: i32 = 4;
    const POLLOUT: u16 = 4;
    const EINPROGRESS: i32 = 115;

    let domain = match addr {
        std::net::SocketAddr::V4(_) => AF_INET,
        std::net::SocketAddr::V6(_) => AF_INET6,
    };
    let fd: i32 = unsafe { socket(domain, SOCK_STREAM, 0) };
    if fd < 0 {
        return Err("socket() failed".into());
    }
    unsafe {
        let fl: i32 = fcntl(fd, F_GETFL, 0);
        let _ = fcntl(fd, F_SETFL, fl | O_NONBLOCK);
    }
    let sa: Vec<u8> = match addr {
        std::net::SocketAddr::V4(v4) => {
            let p = v4.port(); // network order: high byte first
            let ip = v4.ip().octets();
            let mut b = vec![0u8; 16];
            b[0] = 2;
            b[2] = (p >> 8) as u8;
            b[3] = (p & 0xff) as u8;
            b[4..8].copy_from_slice(&ip);
            b
        }
        std::net::SocketAddr::V6(v6) => {
            let p = v6.port();
            let ip = v6.ip().octets();
            let mut b = vec![0u8; 28];
            b[0] = 0; // low byte of AF_INET6 (10) little-endian
            b[1] = 10;
            b[2] = (p >> 8) as u8;
            b[3] = (p & 0xff) as u8;
            b[8..24].copy_from_slice(&ip);
            b[26] = v6.scope_id() as u8;
            b
        }
    };
    let r: i32 = unsafe { connect(fd, sa.as_ptr(), sa.len() as u32) };
    if r < 0 {
        let e: i32 = unsafe { *__errno_location() };
        if e != EINPROGRESS {
            unsafe { close(fd) };
            return Err(format!("connect errno {e}"));
        }
        // EINPROGRESS: poll for writability within timeout
        let mut pfd = PollFd { fd, ev: POLLOUT, re: 0 };
        let ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let n: i32 = unsafe { poll(&mut pfd, 1, ms) };
        if n <= 0 {
            unsafe { close(fd) };
            return Err("connect timeout".into());
        }
        let mut se: i32 = 0;
        let mut l: u32 = std::mem::size_of::<i32>() as u32;
        unsafe {
            if getsockopt(fd, 1 /*SOL_SOCKET*/, 4 /*SO_ERROR*/, &mut se as *mut i32 as *mut i8, &mut l) != 0 || se != 0 {
                close(fd);
                return Err(format!("connect SO_ERROR {se}"));
            }
        }
    }
    unsafe {
        let fl: i32 = fcntl(fd, F_GETFL, 0);
        let _ = fcntl(fd, F_SETFL, fl & !O_NONBLOCK);
    }
    unsafe { Ok(TcpStream::from_raw_fd(fd)) }
}

#[cfg(not(unix))]
fn connect_bounded(addr: std::net::SocketAddr, _timeout: std::time::Duration) -> Result<TcpStream, String> {
    TcpStream::connect(addr).map_err(|e| format!("{e}"))
}

fn read_line(r: &mut TcpStream, buf: &mut Vec<u8>, cap: usize, deadline: Instant) -> Result<Option<String>, String> {
    // NOTE: buf is caller-owned and PERSISTS across calls. Do NOT clear it.
    loop {
        if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let line: String = String::from_utf8_lossy(&buf[..pos]).to_string();
            buf.drain(..=pos);
            return Ok(Some(line.trim_end_matches('\r').to_string()));
        }
        if buf.len() > cap {
            return Err(format!("header line > {cap} bytes (truncated)"));
        }
        if Instant::now() >= deadline {
            return Err("read timeout".into());
        }
        let rem = deadline.saturating_duration_since(Instant::now());
        let _ = r.set_read_timeout(Some(rem));
        let mut tmp = [0u8; 8192];
        match r.read(&mut tmp) {
            Ok(0) => return Ok(None),
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                if Instant::now() >= deadline { return Err("read timeout".into()); }
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline { return Err("read timeout".into()); }
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            Err(e) => return Err(format!("read: {e}")),
        }
    }
}

fn read_exact_n(r: &mut TcpStream, n: usize, out: &mut Vec<u8>, cap: usize, deadline: Instant) -> Result<bool, String> {
    let mut got = out.len(); // out may already hold bytes seeded from the header over-read
    while got < n {
        if out.len() + (n - got) > cap {
            out.resize(out.len() + (n - got).min(4096), 0);
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Err("read timeout".into());
        }
        let rem = deadline.saturating_duration_since(Instant::now());
        let _ = r.set_read_timeout(Some(rem));
        let mut tmp = [0u8; 16384];
        let want = (n - got).min(tmp.len());
        match r.read(&mut tmp[..want]) {
            Ok(0) => return Err("premature eof".into()),
            Ok(x) => { out.extend_from_slice(&tmp[..x]); got += x; }
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                if Instant::now() >= deadline { return Err("read timeout".into()); }
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline { return Err("read timeout".into()); }
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            Err(e) => return Err(format!("read body: {e}")),
        }
    }
    Ok(false)
}


/// Perform one HTTP/1.1 request over `stream`. Returns (Response, whether stream reusable).
/// `drain_cap` limits how many body bytes we read (beyond that, we mark truncated and
/// consider the connection dirty if keep-alive).
pub fn request_h1(
    stream: &mut TcpStream,
    req: &Request,
    timeout: std::time::Duration,
    skip_body: bool,
    drain_cap: usize,
) -> Result<(Response, bool), String> {
    let total_start = Instant::now();
    let deadline = total_start + timeout;
    // Build request line + headers.
    let mut req_bytes = Vec::with_capacity(1024 + req.body.len());
    req_bytes.extend_from_slice(req.method.as_bytes());
    req_bytes.push(b' ');
    req_bytes.extend_from_slice(req.path.as_bytes());
    req_bytes.extend_from_slice(b" HTTP/1.1\r\n");
    req_bytes.extend_from_slice(format!("Host: {}\r\n", req.host).as_bytes());
    for (k, v) in &req.headers {
        req_bytes.extend_from_slice(format!("{k}: {v}\r\n").as_bytes());
    }
    let has_body = !req.body.is_empty();
    if !req.headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("content-length")) && !req.headers.iter().any(|(k,_)| k.eq_ignore_ascii_case("transfer-encoding")) {
        req_bytes.extend_from_slice(format!("Content-Length: {}\r\n", req.body.len()).as_bytes());
    }
    if req.keepalive {
        req_bytes.extend_from_slice(b"Connection: keep-alive\r\n\r\n");
    } else {
        req_bytes.extend_from_slice(b"Connection: close\r\n\r\n");
    }
    req_bytes.extend_from_slice(&req.body);
    let bytes_out = req_bytes.len() as u64;

    stream
        .write_all(&req_bytes)
        .map_err(|e| format!("write: {e}"))?;

    // Read status line.
    let mut line_buf = Vec::with_capacity(4096);
    let status_line = read_line(stream, &mut line_buf, 65536, deadline)
        .map_err(|e| format!("status: {e}"))?
        .ok_or_else(|| "eof on status".to_string())?;
    // TTFB = time to first byte of response (status line arrival).
    let ttfb_us = total_start.elapsed().as_micros() as u64;

    let mut parts = status_line.splitn(3, ' ');
    let _ver = parts.next().unwrap_or("HTTP/1.1");
    let status: u16 = parts.next().unwrap_or("0").parse().unwrap_or(0);
    let _reason = parts.next().unwrap_or("");

    // Read headers.
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    let mut keepalive = true;
    loop {
        let line = read_line(stream, &mut line_buf, 65536, deadline).map_err(|e| format!("header: {e}"))?;
        let Some(line) = line else { break };
        if line.is_empty() {
            break;
        }
        if let Some(colon) = line.find(':') {
            let k = line[..colon].to_string();
            let v = line[colon + 1..].trim().to_string();
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.parse::<usize>().ok();
            } else if k.eq_ignore_ascii_case("transfer-encoding") {
                if v.to_lowercase().contains("chunked") {
                    chunked = true;
                }
            } else if k.eq_ignore_ascii_case("connection") {
                keepalive = !v.to_lowercase().contains("close");
            }
            headers.push((k, v));
        }
    }

    let mut body = Vec::new();
    // RFC 9112/9110: HEAD and 1xx/204/304 responses have no message body.
    // Treating them as close-delimited would block or poison a keep-alive socket.
    let body_forbidden = req.method.eq_ignore_ascii_case("HEAD")
        || (100..200).contains(&status)
        || status == 204
        || status == 304;
    // line_buf may hold over-read body bytes (the socket read that fetched the
    // status+headers can also pull in part/all of the body). Seed body with the
    // leftover so read_exact_n/read_chunked don't block on the socket for bytes
    // we already have.
    body.extend_from_slice(&line_buf);
    line_buf.clear();
    let mut truncated = false;
    let mut bytes_in = (status_line.len() as u64) + (headers.len() as u64 * 20);
    if body_forbidden {
        body.clear();
        // Any bytes already over-read belong to the next response only when the
        // protocol guarantees no body; for these response classes there must be
        // none. Keep the connection reusable and ignore an empty parser buffer.
    } else if skip_body {
        // Don't read body; the stream cannot be safely reused because unread bytes
        // would be interpreted as the next response.
        keepalive = false;
    } else if chunked {
        match read_chunked(stream, &mut body, drain_cap, deadline) {
            Ok(t) => truncated = t,
            Err(e) => return Err(format!("chunked: {e}")),
        }
        // connection dirty if truncated (we didn't read full body)
        if truncated {
            keepalive = false;
        }
    } else if let Some(cl) = content_length {
        if cl == 0 {
            // no body
        } else {
            match read_exact_n(stream, cl, &mut body, drain_cap, deadline) {
                Ok(t) => truncated = t,
                Err(e) => return Err(format!("body: {e}")),
            }
            if truncated {
                keepalive = false;
            }
        }
    } else {
        // read until close (no length, not chunked) -> connection not reusable
        loop {
            let mut tmp = [0u8; 16384];
            match stream.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => {
                    if body.len() + n > drain_cap {
                        body.extend_from_slice(&tmp[..drain_cap.saturating_sub(body.len()).max(0).min(n)]);
                        truncated = true;
                        keepalive = false;
                        break;
                    }
                    body.extend_from_slice(&tmp[..n]);
                }
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                    truncated = true;
                    keepalive = false;
                    break;
                }
                Err(e) => return Err(format!("body-closeline: {e}")),
            }
        }
        keepalive = false;
    }

    bytes_in += body.len() as u64;
    let total_us = total_start.elapsed().as_micros() as u64;

    Ok((
        Response {
            status,
            headers,
            body,
            ttfb_us,
            total_us,
            truncated,
            keepalive,
            bytes_in,
            bytes_out,
        },
        keepalive,
    ))
}

fn read_chunked(r: &mut TcpStream, body: &mut Vec<u8>, cap: usize, deadline: Instant) -> Result<bool, String> {
    let mut truncated = false;
    let mut line_buf = Vec::with_capacity(1024);
    loop {
        let line = read_line(r, &mut line_buf, 65536, deadline).map_err(|e| format!("chunk-len: {e}"))?;
        let Some(line) = line else {
            truncated = true;
            break;
        };
        let size_str = line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_str, 16).map_err(|_| format!("bad chunk size {size_str}"))?;
        if size == 0 {
            // consume trailing CRLF / trailers
            let _ = read_line(r, &mut line_buf, 65536, deadline);
            break;
        }
        if truncated {
            break;
        }
        if body.len() + size > cap {
            // drain remainder of this chunk but don't store
            let mut drain = [0u8; 16384];
            let mut got = 0usize;
            while got < size {
                let want = (size - got).min(drain.len());
                match r.read(&mut drain[..want]) {
                    Ok(0) => return Err("premature eof chunk".into()),
                    Ok(n) => got += n,
                    Err(e) if e.kind() == std::io::ErrorKind::TimedOut => return Err("timeout chunk".into()),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }
                    Err(e) => return Err(format!("chunk: {e}")),
                }
            }
            let _ = read_line(r, &mut line_buf, 65536, deadline); // CRLF
            truncated = true;
            break;
        }
        // read size bytes + CRLF
        let mut tmp = [0u8; 16384];
        let mut got = 0usize;
        while got < size {
            let want = (size - got).min(tmp.len());
            match r.read(&mut tmp[..want]) {
                Ok(0) => return Err("premature eof chunk body".into()),
                Ok(n) => {
                    body.extend_from_slice(&tmp[..n]);
                    got += n;
                }
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => return Err("timeout chunk body".into()),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
                Err(e) => return Err(format!("chunk body: {e}")),
            }
        }
        // consume CRLF after chunk
        let mut crlf = [0u8; 2];
        read_exact(r, &mut crlf, deadline).map_err(|e| format!("chunk crlf: {e}"))?;
    }
    Ok(truncated)
}

fn read_exact(r: &mut TcpStream, buf: &mut [u8], deadline: Instant) -> Result<(), String> {
    let mut got = 0usize;
    while got < buf.len() {
        if Instant::now() >= deadline {
            return Err("timeout".into());
        }
        let rem = deadline.saturating_duration_since(Instant::now());
        let _ = r.set_read_timeout(Some(rem));
        match r.read(&mut buf[got..]) {
            Ok(0) => return Err("eof".into()),
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                if Instant::now() >= deadline { return Err("timeout".into()); }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline { return Err("timeout".into()); }
                std::thread::yield_now();
            }
            Err(e) => return Err(format!("read exact: {e}")),
        }
    }
    Ok(())
}
}
}

mod rate {
// Rate schedulers.
// - Worker rate: per-thread token bucket (independent; sums to N*rps across N workers).
// - Arrival rate: global Poisson arrival process with a bounded queue + explicit
//   backpressure policy (drop / block / error). Drop counter is exposed (no silent loss).

use std::time::Duration;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backpressure {
    Drop,   // drop + count
    Block,  // block until slot (count blocked time)
    Error,  // count as backpressure error and stop the load
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Per-worker token bucket.
pub struct TokenBucket {
    rate: f64,
    capacity: f64,
    tokens: f64,
    last: Instant,
    dropped: u64,
}

impl TokenBucket {
    pub fn new(rate: f64, burst: f64) -> Self {
        let capacity = if burst > 0.0 { burst } else { rate.max(1.0) };
        Self { rate, capacity, tokens: if burst > 0.0 { burst.min(capacity) } else { 0.0 }, last: Instant::now(), dropped: 0 }
    }

    pub fn is_limited(&self) -> bool {
        self.rate > 0.0
    }

    /// Block until a token is available; returns microseconds waited.
    pub fn acquire(&mut self) -> u64 {
        if self.rate <= 0.0 {
            return 0;
        }
        let start = Instant::now();
        loop {
            self.refill();
            if self.tokens >= 1.0 {
                self.tokens -= 1.0;
                return start.elapsed().as_micros() as u64;
            }
            let need = 1.0 - self.tokens;
            let wait = need / self.rate;
            std::thread::sleep(Duration::from_secs_f64(wait.min(0.05)));
        }
    }

    pub fn try_acquire(&mut self) -> bool {
        if self.rate <= 0.0 {
            return true;
        }
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            self.dropped += 1;
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let dt = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + dt * self.rate).min(self.capacity);
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}

/// Minimal internal bounded channel (no external dep).
pub mod cross_channel {
    use std::collections::VecDeque;
    use std::sync::{Arc, Condvar, Mutex};

    struct Inner {
        q: VecDeque<()>,
        cap: usize,
        closed: bool,
    }
    pub struct Channel {
        inner: Arc<Mutex<Inner>>,
        cv: Arc<Condvar>,
    }
    impl Clone for Channel {
        fn clone(&self) -> Self {
            Self { inner: Arc::clone(&self.inner), cv: Arc::clone(&self.cv) }
        }
    }
    impl Channel {
        pub fn new(cap: usize) -> Self {
            Self {
                inner: Arc::new(Mutex::new(Inner { q: VecDeque::new(), cap: cap.max(1), closed: false })),
                cv: Arc::new(Condvar::new()),
            }
        }
        pub fn send(&self) -> bool {
            let mut g = self.inner.lock().unwrap();
            loop {
                if g.closed {
                    return false;
                }
                if g.q.len() < g.cap {
                    g.q.push_back(());
                    self.cv.notify_one();
                    return true;
                }
                g = self.cv.wait(g).unwrap();
            }
        }
        pub fn try_send(&self) -> bool {
            let mut g = self.inner.lock().unwrap();
            if g.closed || g.q.len() >= g.cap {
                return false;
            }
            g.q.push_back(());
            self.cv.notify_one();
            true
        }
        pub fn recv_timeout(&self, d: std::time::Duration) -> bool {
            let mut g = self.inner.lock().unwrap();
            let deadline = std::time::Instant::now() + d;
            loop {
                if g.q.pop_front().is_some() {
                    return true;
                }
                if g.closed {
                    return false;
                }
                let now = std::time::Instant::now();
                if now >= deadline {
                    return false;
                }
                g = self.cv.wait_timeout(g, deadline - now).unwrap().0;
            }
        }
        pub fn close(&self) {
            let mut g = self.inner.lock().unwrap();
            g.closed = true;
            self.cv.notify_all();
        }
        pub fn len(&self) -> usize {
            self.inner.lock().unwrap().q.len()
        }
    }
}

/// Global Poisson arrival pacer + bounded queue + backpressure counters.
pub struct Arrival {
    pub chan: cross_channel::Channel,
    pub stop: Arc<AtomicBool>,
    pub dropped_tokens: Arc<AtomicU64>,
    pub blocked_us: Arc<AtomicU64>,
    _pacer: Option<std::thread::JoinHandle<()>>,
}

impl Arrival {
    pub fn start(rate: f64, queue_cap: usize, policy: Backpressure, stop: Arc<AtomicBool>) -> Self {
        let chan = cross_channel::Channel::new(queue_cap);
        let dropped = Arc::new(AtomicU64::new(0));
        let blocked = Arc::new(AtomicU64::new(0));
        let chan2 = chan.clone();
        let stop2 = stop.clone();
        let dropped2 = dropped.clone();
        let blocked2 = blocked.clone();
        let pacer = std::thread::Builder::new()
            .name("arrival-pacer".into())
            .spawn(move || {
                let mut rng = SimpleRng::new();
                loop {
                    if stop2.load(Ordering::Relaxed) {
                        break;
                    }
                    let u = rng.next();
                    // Poisson inter-arrival = -ln(U)/rate. Compute the positive magnitude
                    // explicitly: -(u.ln()) is >= 0 for u in (0,1); guard against 0/neg.
                    let neg_ln = -(u.ln());
                    let inter = neg_ln.max(1e-9) / rate;
                    let mut remaining = Duration::from_secs_f64(inter);
                    while remaining.as_nanos() > 0 && !stop2.load(Ordering::Relaxed) {
                        let chunk = remaining.min(Duration::from_millis(1));
                        std::thread::sleep(chunk);
                        remaining = remaining.saturating_sub(chunk);
                    }
                    if stop2.load(Ordering::Relaxed) {
                        break;
                    }
                    let start = Instant::now();
                    if chan2.try_send() {
                        // ok
                    } else {
                        match policy {
                            Backpressure::Drop => {
                                dropped2.fetch_add(1, Ordering::Relaxed);
                            }
                            Backpressure::Block => {
                                let _ = chan2.send();
                                blocked2.fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
                            }
                            Backpressure::Error => {
                                dropped2.fetch_add(1, Ordering::Relaxed);
                                stop2.store(true, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                }
                chan2.close();
            })
            .expect("spawn pacer");
        Self { chan, stop, dropped_tokens: dropped, blocked_us: blocked, _pacer: Some(pacer) }
    }

    /// Worker side: wait for a fire signal. Returns false if pacer stopped.
    pub fn wait_fire(&self, poll: Duration) -> bool {
        self.chan.recv_timeout(poll)
    }
}

/// xorshift64* PRNG for Poisson inter-arrival.
pub struct SimpleRng {
    state: u64,
}
impl SimpleRng {
    pub fn new() -> Self {
        let mut s = (now_nanos() as u64) ^ (std::process::id() as u64).wrapping_mul(0x9E3779B97F4A7C15);
        if s == 0 {
            s = 0xDEADBEEF;
        }
        Self { state: s }
    }
    pub fn next(&mut self) -> f64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        let v = x.wrapping_mul(0x2545F4914F6CDD1D);
        ((v >> 11) as f64) / (1u64 << 53) as f64 + 1e-9
    }
}
}

mod stats {
// Per-worker statistics. Lock-free within a worker (single thread), merged at exit.
// Tracks: counts, latency + TTFB histograms, in-flight, status codes, error taxonomy,
// per-endpoint counts, bytes, connect times, truncated-body count, H2 pushes.

use super::histogram::Histogram;
use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug)]
pub struct Stats {
    pub start: Instant,
    pub requests: u64,
    pub success: u64,
    pub failed: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub connect_count: u64,
    pub connect_us_sum: u64,
    pub h2_pushes: u64,
    pub truncated: u64,
    pub dropped_tokens: u64,
    pub blocked_us: u64,
    pub in_flight: usize,
    pub peak_in_flight: usize,
    pub latency: Histogram,
    pub ttfb: Histogram,
    pub status_codes: HashMap<u16, u64>,
    pub errors: HashMap<String, u64>,
    pub per_endpoint: HashMap<u32, u64>, // endpoint id -> requests
    // timeline sampling (second -> (rps, p50, p99))
    pub timeline: Vec<(f64, f64, f64)>,
    // warmup (pre-recording) counter
    pub warmup_requests: u64,
    pub recording: bool,
}

impl Stats {
    pub fn new() -> Self {
        let s = Self {
            start: Instant::now(),
            requests: 0,
            success: 0,
            failed: 0,
            bytes_in: 0,
            bytes_out: 0,
            connect_count: 0,
            connect_us_sum: 0,
            h2_pushes: 0,
            truncated: 0,
            dropped_tokens: 0,
            blocked_us: 0,
            in_flight: 0,
            peak_in_flight: 0,
            latency: Histogram::new(),
            ttfb: Histogram::new(),
            status_codes: HashMap::new(),
            errors: HashMap::new(),
            per_endpoint: HashMap::new(),
            timeline: Vec::new(),
            warmup_requests: 0,
            recording: true,
        };
        s
    }

    pub fn note_inflight(&mut self, delta: i32) -> usize {
        self.in_flight = (self.in_flight as i32 + delta).max(0) as usize;
        if self.in_flight > self.peak_in_flight {
            self.peak_in_flight = self.in_flight;
        }
        self.in_flight
    }

    pub fn record_success(
        &mut self,
        latency_us: u64,
        ttfb_us: u64,
        status: u16,
        body_bytes: u64,
        out_bytes: u64,
        endpoint: u32,
        truncated: bool,
    ) {
        if !self.recording {
            self.warmup_requests += 1;
            return;
        }
        self.requests += 1;
        self.success += 1;
        self.bytes_in += body_bytes;
        self.bytes_out += out_bytes;
        self.latency.record(latency_us);
        self.ttfb.record(ttfb_us);
        *self.status_codes.entry(status).or_insert(0) += 1;
        *self.per_endpoint.entry(endpoint).or_insert(0) += 1;
        if truncated {
            self.truncated += 1;
        }
    }

    pub fn record_failure(&mut self, latency_us: u64, error: &str, endpoint: u32) {
        if !self.recording {
            self.warmup_requests += 1;
            return;
        }
        self.requests += 1;
        self.failed += 1;
        self.latency.record(latency_us);
        *self.errors.entry(error.to_string()).or_insert(0) += 1;
        *self.per_endpoint.entry(endpoint).or_insert(0) += 1;
    }

    pub fn record_connect(&mut self, us: u64) {
        self.connect_count += 1;
        self.connect_us_sum += us;
    }

    pub fn error_rate(&self) -> f64 {
        if self.requests == 0 {
            0.0
        } else {
            self.failed as f64 / self.requests as f64
        }
    }

    pub fn merge(&mut self, other: &Stats) {
        self.requests += other.requests;
        self.success += other.success;
        self.failed += other.failed;
        self.bytes_in += other.bytes_in;
        self.bytes_out += other.bytes_out;
        self.connect_count += other.connect_count;
        self.connect_us_sum += other.connect_us_sum;
        self.h2_pushes += other.h2_pushes;
        self.truncated += other.truncated;
        self.dropped_tokens += other.dropped_tokens;
        self.blocked_us += other.blocked_us;
        self.warmup_requests += other.warmup_requests;
        self.peak_in_flight = self.peak_in_flight.max(other.peak_in_flight);
        self.latency.merge(&other.latency);
        self.ttfb.merge(&other.ttfb);
        for (k, v) in &other.status_codes {
            *self.status_codes.entry(*k).or_insert(0) += v;
        }
        for (k, v) in &other.errors {
            *self.errors.entry(k.clone()).or_insert(0) += v;
        }
        for (k, v) in &other.per_endpoint {
            *self.per_endpoint.entry(*k).or_insert(0) += v;
        }
        self.timeline.extend(other.timeline.iter().cloned());
    }
}
}

mod worker {
// Worker thread: connection management + keep-alive reuse + per-request timing,
// circuit breaker, rate control, error taxonomy, optional CSV sampling.

use super::breaker::Breaker;
use super::config::{Config, RateMode, Scheme};
use super::http1::{self, Request};
use super::rate::{Arrival, TokenBucket};
use super::stats::Stats;
use std::io::Read;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct WorkerParams {
    pub cfg: Config,
    pub breaker: Breaker,
    pub stop: Arc<AtomicBool>,
    pub arrival: Option<Arc<Arrival>>,
    pub endpoint_ids: Vec<(String, u16)>, // (host, port) per endpoint
    pub ep_for_target: Vec<Vec<u32>>,     // per target index -> endpoint ids
    pub csv: Option<std::sync::Arc<std::sync::Mutex<std::fs::File>>>,
    pub warmup: Duration,
    pub duration: Duration,
    pub report_interval: Duration,
}

fn host_with_port(host: &str, port: u16, scheme: &Scheme) -> String {
    let default = match scheme {
        Scheme::Http => 80u16,
        Scheme::Https => 443,
    };
    if port == default {
        host.to_string()
    } else {
        format!("{host}:{port}")
    }
}

pub fn run_worker(p: WorkerParams, worker_id: usize) -> Stats {
    let cfg = &p.cfg;
    let mut stats = Stats::new();
    let drain_cap = 256 * 1024;
    let timeout = cfg.timeout;

    // Per-worker rate control.
    let mut bucket = if cfg.rate_mode == RateMode::Worker && cfg.rps > 0.0 {
        let per = cfg.rps / cfg.workers.max(1) as f64;
        Some(TokenBucket::new(per, if cfg.rps_burst > 0.0 { cfg.rps_burst / cfg.workers.max(1) as f64 } else { 0.0 }))
    } else {
        None
    };

    let arrival = p.arrival.clone();
    let stop = p.stop.clone();

    // Connection pool: one stream per endpoint id (keep-alive), None if broken.
    let mut conns: Vec<Option<TcpStream>> = (0..p.endpoint_ids.len()).map(|_| None).collect();

    // Warmup gate.
    let t0 = Instant::now();
    if p.warmup > Duration::ZERO {
        stats.recording = false;
    }

    // Round-robin / random state.
    let mut rr = worker_id;
    let mut rng = super::rate::SimpleRng::new();

    let _deadline = t0 + p.duration;

    let mut csv_line: Vec<u8> = Vec::with_capacity(256);

    loop {
        if stop.load(Ordering::Relaxed) {
            if std::env::var("SC_DEBUG").is_ok() { eprintln!("[w{}] break via stop", worker_id); }
            break;
        }
        if t0.elapsed() >= p.duration {
            if std::env::var("SC_DEBUG").is_ok() { eprintln!("[w{}] break via duration elapsed={:?}", worker_id, t0.elapsed()); }
            break;
        }
        if !stats.recording && t0.elapsed() >= p.warmup {
            stats.recording = true;
            // keep going
        }

        // Rate gate.
        match cfg.rate_mode {
            RateMode::Worker => {
                if let Some(b) = &mut bucket {
                    if !b.try_acquire() {
                        // drop policy in worker mode: just skip this iter (count)
                        stats.dropped_tokens += 1;
                        continue;
                    }
                }
            }
            RateMode::Arrival => {
                if let Some(a) = &arrival {
                    if !a.wait_fire(Duration::from_millis(50)) {
                        continue;
                    }
                }
            }
        }

        // Pick target (randomize across targets), then endpoint within target.
        let target_idx = if cfg.mode == super::config::TargetMode::Random {
            (rng.next() * cfg.targets.len() as f64) as usize % cfg.targets.len()
        } else {
            let i = rr % cfg.targets.len();
            rr += 1;
            i
        };
        let eps = &p.ep_for_target[target_idx];
        if eps.is_empty() {
            continue;
        }
        let ep_local = if cfg.mode == super::config::TargetMode::Random {
            (rng.next() * eps.len() as f64) as usize % eps.len()
        } else {
            let i = rr % eps.len();
            i
        };
        let ep_global = eps[ep_local];

        // Circuit breaker gate.
        if !p.breaker.ready(ep_global) {
            // endpoint open: count a failure (skipped) and move on
            continue;
        }

        // Ensure connection.
        let (host, port) = &p.endpoint_ids[ep_global as usize];
        let (host_owned, port_owned) = (host.clone(), *port);
        let needs_new = conns[ep_global as usize].is_none();
        if needs_new {
            let (s, us) = http1::connect_us(&host_owned, port_owned, timeout);
            match s {
                Some(stream) => {
                    stats.record_connect(us);
                    conns[ep_global as usize] = Some(stream);
                }
                None => {
                    stats.record_failure(us, "connect_error", ep_global);
                    p.breaker.mark_fail(ep_global);
                    continue;
                }
            }
        }

        // Build request.
        let req = Request {
            method: cfg.method.clone(),
            host: host_with_port(&host_owned, port_owned, &cfg.scheme),
            authority: (host_owned, port_owned),
            path: cfg.path.clone(),
            headers: cfg.headers.clone(),
            body: cfg.body.clone(),
            scheme: cfg.scheme,
        };

        let mut dirty = false;
        let start = Instant::now();
        let req_out_bytes = (req.method.len() + req.path.len() + req.body.len() + 200) as u64;
        let result = match conns[ep_global as usize].as_mut() {
            Some(stream) => http1::request_h1(stream, &req, timeout, cfg.skip_body, drain_cap),
            None => Err("no conn".to_string()),
        };

        match result {
            Ok((resp, keepalive)) => {
                if std::env::var("SC_DEBUG").is_ok() {
                    eprintln!("[w{} #{}] got status={} keepalive={}", worker_id, stats.requests + stats.failed, resp.status, keepalive);
                }
                let latency_us = start.elapsed().as_micros() as u64;
                let is_success = cfg.ignore_status || resp.status < 400; // [FIX4] 对齐 Python 口径: status<400 为成功, 400-499 计失败
                if is_success {
                    stats.record_success(
                        latency_us,
                        resp.ttfb_us,
                        resp.status,
                        resp.body.len() as u64,
                        resp.bytes_out,
                        ep_global,
                        resp.truncated,
                    );
                    p.breaker.mark_ok(ep_global);
                    // CSV sampling
                    if let Some(file) = &p.csv {
                        if stats.requests % cfg.sample_every == 0 {
                            csv_line.clear();
                            write!(
                                csv_line,
                                "{},{},{},{},{},{}\n",
                                worker_id,
                                resp.status,
                                latency_us,
                                resp.ttfb_us,
                                ep_global,
                                resp.body.len()
                            )
                            .ok();
                            let mut g = file.lock().unwrap();
                            use std::io::Write;
                            g.write_all(&csv_line).ok();
                        }
                    }
                } else {
                    stats.record_success(
                        latency_us,
                        resp.ttfb_us,
                        resp.status,
                        resp.body.len() as u64,
                        resp.bytes_out,
                        ep_global,
                        resp.truncated,
                    );
                    // non-success status counts as failed request
                    stats.success -= 1;
                    stats.failed += 1;
                }
                if !keepalive || resp.truncated {
                    dirty = true;
                }
            }
            Err(e) => {
                let latency_us = start.elapsed().as_micros() as u64;
                let cat = categorize_error(&e);
                stats.record_failure(latency_us, &cat, ep_global);
                p.breaker.mark_fail(ep_global);
                dirty = true;
            }
        }

        // Close connection if dirty.
        if dirty || conns[ep_global as usize].is_none() {
            if let Some(mut s) = conns[ep_global as usize].take() {
                let _ = s.set_read_timeout(None);
                // drain residual to allow clean FIN (best effort)
                let mut drain = [0u8; 4096];
                let _ = s.read(&mut drain);
            }
        }

        // _ = req_out_bytes reserved for future bytes_out refinement
        let _ = req_out_bytes;
    }

    // Close remaining conns.
    for c in conns.iter_mut() {
        if c.take().is_some() {}
    }

    if std::env::var("SC_DEBUG").is_ok() {
        eprintln!("[w{}] EXIT total={}", worker_id, stats.requests);
    }
    stats
}

fn categorize_error(e: &str) -> &'static str {
    let l = e.to_lowercase();
    if l.contains("timeout") {
        "timeout"
    } else if l.contains("resolve") || l.contains("dns") {
        "dns"
    } else if l.contains("connect") || l.contains("refused") || l.contains("reset") {
        "conn_error"
    } else if l.contains("eof") {
        "eof"
    } else if l.contains("write") {
        "write_error"
    } else if l.contains("chunk") || l.contains("body") || l.contains("header") {
        "protocol_error"
    } else {
        "other"
    }
}
}

// stresscore — TaichuAgentTeam HTTP stress data-plane core.
// Zero external dependencies (std only). Control-plane (Python) drives this binary.
//
// Usage:
//   stresscore --target 127.0.0.1:8080 --path / --workers 100 --duration 30
//   stresscore --targets-file targets.txt --rps 10000 --rate-mode arrival --json


use breaker::Breaker;
use config::{Backpressure, Config, RateMode, Scheme, TargetMode};
use stats::Stats;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const VERSION: &str = "34.0.0";

/// Format a float with `prec` decimals (portable across rustc versions).
fn f2(v: f64) -> String {
    format!("{:.2}", v)
}
fn f3(v: f64) -> String {
    format!("{:.3}", v)
}

struct Cli {
    cfg: Config,
    backpressure: Backpressure,
    csv_path: Option<String>,
    json_out: bool,
    show_version: bool,
}

fn parse_args() -> Result<Cli, String> {
    let args: Vec<String> = std::env::args().collect();
    let mut cfg = Config::default();
    let mut backpressure = Backpressure::Drop;
    let mut json_out = false;
    let mut csv_path: Option<String> = None;
    let mut show_version = false;

    let mut i = 1;
    while i < args.len() {
        let a = &args[i];
        let mut next = || {
            i += 1;
            args.get(i).cloned().ok_or_else(|| format!("missing value for {a}"))
        };
        match a.as_str() {
            "--target" | "-t" => {
                cfg.targets.push(next()?);
            }
            "--targets-file" | "-f" => {
                let path = next()?;
                let content = std::fs::read_to_string(&path).map_err(|e| format!("read {path}: {e}"))?;
                for line in content.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    for part in line.split(',') {
                        let part = part.trim();
                        if !part.is_empty() {
                            cfg.targets.push(part.to_string());
                        }
                    }
                }
            }
            "--path" | "-p" => cfg.path = next()?,
            "--method" | "-m" => cfg.method = next()?,
            "--body" | "-b" => cfg.body = next()?.into_bytes(),
            "--header" | "-H" => {
                let h = next()?;
                if let Some(colon) = h.find(':') {
                    cfg.headers.push((h[..colon].to_string(), h[colon + 1..].trim().to_string()));
                }
            }
            "--workers" | "-w" => cfg.workers = next()?.parse::<usize>().map_err(|_| String::from("workers not int"))?,
            "--duration" | "-d" => cfg.duration = config::dur_secs(next()?.parse::<f64>().map_err(|_| String::from("duration not num"))?),
            "--warmup" => cfg.warmup = config::dur_secs(next()?.parse::<f64>().map_err(|_| String::from("warmup not num"))?),
            "--timeout" | "-T" => cfg.timeout = config::dur_secs(next()?.parse::<f64>().map_err(|_| String::from("timeout not num"))?),
            "--mode" => {
                let m = next()?;
                cfg.mode = if m == "random" { TargetMode::Random } else { TargetMode::RoundRobin };
            }
            "--rps" => cfg.rps = next()?.parse::<f64>().map_err(|_| String::from("rps not num"))?,
            "--rate-mode" => {
                let m = next()?;
                cfg.rate_mode = if m == "arrival" { RateMode::Arrival } else { RateMode::Worker };
            }
            "--backpressure" => {
                let m = next()?;
                backpressure = match m.as_str() {
                    "block" => Backpressure::Block,
                    "error" => Backpressure::Error,
                    _ => Backpressure::Drop,
                };
            }
            "--rps-burst" => cfg.rps_burst = next()?.parse::<f64>().map_err(|_| String::from("rps-burst not num"))?,
            "--no-keepalive" => cfg.keepalive = false,
            "--insecure" => cfg.insecure = true,
            "--scheme" => {
                let s = next()?;
                cfg.scheme = if s == "https" { Scheme::Https } else { Scheme::Http };
            }
            "--skip-body" => cfg.skip_body = true,
            "--ignore-status" => cfg.ignore_status = true,
            "--interval" => cfg.report_interval = config::dur_secs(next()?.parse::<f64>().map_err(|_| String::from("interval not num"))?),
            "--sample-every" => cfg.sample_every = next()?.parse::<u64>().map_err(|_| String::from("sample not int"))?,
            "--csv" => {
                csv_path = Some(next()?);
                cfg.sample_every = cfg.sample_every.max(1);
            }
            "--ep-breaker" => cfg.ep_breaker = next()?.parse::<u32>().map_err(|_| String::from("ep-breaker not int"))?,
            "--ep-recovery" => cfg.ep_recovery = config::dur_secs(next()?.parse::<f64>().map_err(|_| String::from("ep-recovery not num"))?),
            "--json" => json_out = true,
            "--version" => show_version = true,
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                if other.starts_with("--") {
                    return Err(format!("unknown arg {other}"));
                }
            }
        }
        i += 1;
    }
    if !show_version && cfg.targets.is_empty() {
        return Err("no targets (use --target or --targets-file)".into());
    }
    if cfg.workers == 0 {
        return Err("workers must be > 0".into());
    }
    if cfg.timeout.is_zero() {
        return Err("timeout must be > 0".into());
    }
    if cfg.rate_mode == RateMode::Arrival && cfg.rps <= 0.0 {
        return Err("arrival rate mode requires rps > 0".into());
    }
    Ok(Cli { cfg, backpressure, csv_path, json_out, show_version })
}

fn print_help() {
    eprint!(
        "stresscore v{VERSION} — TaichuAgentTeam HTTP/1.1 stress core (zero-dep, std only)\n
Options:
  -t, --target HOST:PORT     Target (repeatable)
  -f, --targets-file PATH    File of targets (one per line or comma list)
  -p, --path PATH            Request path (default /)
  -m, --method METHOD        HTTP method (default GET)
  -b, --body TEXT            Request body
  -H, --header K:V           Extra header (repeatable)
  -w, --workers N            Worker threads (default 50)
  -d, --duration SECS        Test duration (default 30)
      --warmup SECS          Warmup before recording stats
  -T, --timeout SECS         Per-request timeout (default 10)
      --mode rr|random       Target selection
      --rps N                Rate limit (0=unlimited)
      --rate-mode worker|arrival
      --backpressure drop|block|error
      --rps-burst N
      --no-keepalive
      --insecure
      --scheme http|https
      --skip-body
      --ignore-status
      --interval SECS        Report interval (default 2)
      --sample-every N       CSV sample rate
      --csv PATH             Sampled per-request CSV
      --ep-breaker N         Endpoint circuit breaker (consecutive fails)
      --ep-recovery SECS
      --json                 Emit final summary as single JSON line
      --version
"
    );
}

fn parse_target(raw: &str, scheme: &Scheme) -> (String, u16) {
    // Accept: host, host:port, scheme://host:port
    let t = raw.trim().trim_matches('"').trim_matches('\'');
    if let Some(rest) = t.strip_prefix("http://") {
        let (h, p) = split_hostport(rest, *scheme);
        return (h, p);
    }
    if let Some(rest) = t.strip_prefix("https://") {
        let (h, p) = split_hostport(rest, Scheme::Https);
        return (h, p);
    }
    split_hostport(t, *scheme)
}

fn split_hostport(s: &str, scheme: Scheme) -> (String, u16) {
    let default = match scheme {
        Scheme::Http => 80u16,
        Scheme::Https => 443,
    };
    // strip path if present
    let hostport = match s.find('/') {
        Some(idx) => &s[..idx],
        None => s,
    };
    if let Some(colon) = hostport.rfind(':') {
        let h = &hostport[..colon];
        let p: u16 = hostport[colon + 1..].parse().unwrap_or(default);
        (h.to_string(), p)
    } else {
        (hostport.to_string(), default)
    }
}

fn main() {
    let cli = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };
    if cli.show_version {
        println!("stresscore {VERSION}");
        return;
    }
    let mut cfg = cli.cfg;
    let t0 = Instant::now();

    // Build endpoint list from targets.
    let mut endpoints: Vec<(String, u16)> = Vec::new();
    let mut ep_for_target: Vec<Vec<u32>> = Vec::new();
    for target in &cfg.targets {
        let (host, port) = parse_target(target, &cfg.scheme);
        let existing = endpoints.iter().position(|(h, p)| h == &host && *p == port);
        let ep = match existing {
            Some(idx) => idx as u32,
            None => {
                endpoints.push((host, port));
                (endpoints.len() - 1) as u32
            }
        };
        ep_for_target.push(vec![ep]);
    }
    if endpoints.is_empty() {
        eprintln!("error: no endpoints");
        std::process::exit(2);
    }

    // CSV file (shared across workers).
    let csv: Option<Arc<std::sync::Mutex<std::fs::File>>> = match &cli.csv_path {
        Some(path) => {
            let f = std::fs::File::create(path).unwrap_or_else(|e| {
                eprintln!("error: create csv {path}: {e}");
                std::process::exit(2);
            });
            let mut f = std::sync::Mutex::new(f);
            let mut g = f.lock().unwrap();
            writeln!(g, "worker,status,latency_us,ttfb_us,endpoint,body_bytes").ok();
            drop(g);
            cfg.sample_every = cfg.sample_every.max(1);
            Some(Arc::new(f))
        }
        None => None,
    };

    let stop = Arc::new(AtomicBool::new(false));
    // Signal handling: first Ctrl+C -> graceful stop; second -> abort.
    install_sigint(&stop);

    // Arrival pacer (global) if arrival mode.
    let arrival = if cfg.rate_mode == RateMode::Arrival && cfg.rps > 0.0 {
        Some(Arc::new(rate::Arrival::start(
            cfg.rps,
            cfg.workers * 64,
            cli.backpressure,
            stop.clone(),
        )))
    } else {
        None
    };

    let breaker = Breaker::new(breaker::BreakerCfg {
        failure_threshold: cfg.ep_breaker,
        recovery: cfg.ep_recovery,
    });

    // Spawn workers.
    let mut handles = Vec::with_capacity(cfg.workers);
    for w in 0..cfg.workers {
        let params = worker::WorkerParams {
            cfg: cfg.clone(),
            breaker: breaker.clone(),
            stop: stop.clone(),
            arrival: arrival.clone(),
            endpoint_ids: endpoints.clone(),
            ep_for_target: ep_for_target.clone(),
            csv: csv.clone(),
            warmup: cfg.warmup,
            duration: cfg.duration,
            report_interval: cfg.report_interval,
        };
        let h = std::thread::Builder::new()
            .name(format!("stress-w{w}"))
            .spawn(move || worker::run_worker(params, w))
            .expect("spawn worker");
        handles.push(h);
    }

    // Reporter thread (live progress to stderr).
    let report_stop = stop.clone();
    let reporter = std::thread::Builder::new()
        .name("reporter".into())
        .spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(500));
                if report_stop.load(Ordering::Relaxed) { break; }
            }
        })
        .expect("spawn reporter");

    // Wait for workers.
    let mut merged = Stats::new();
    merged.recording = true;
    for h in handles {
        if let Ok(s) = h.join() {
            merged.merge(&s);
        }
    }
    stop.store(true, Ordering::Relaxed);
    let _ = reporter.join();

    let elapsed = t0.elapsed().as_secs_f64();
    // Arrival backpressure metrics.
    if let Some(a) = &arrival {
        merged.dropped_tokens += a.dropped_tokens.load(Ordering::Relaxed);
        merged.blocked_us += a.blocked_us.load(Ordering::Relaxed);
    }

    let (open, half, closed) = breaker.snapshot();

    if cli.json_out {
        print_json(&merged, elapsed, open, half, closed);
    } else {
        print_report(&merged, elapsed, open, half, closed);
    }
}

fn print_report(s: &Stats, elapsed: f64, open: usize, half: usize, closed: usize) {
    let rps = s.requests as f64 / elapsed.max(1e-6);
    println!("============================================================");
    println!("SUMMARY  (stresscore v{VERSION})");
    println!("============================================================");
    println!("Duration         : {}s", f2(elapsed));
    println!("Total requests   : {}", s.requests);
    println!("Avg RPS          : {}", f2(rps));
    println!("Success          : {}", s.success);
    println!("Failed           : {}", s.failed);
    println!("Error rate       : {}", format!("{:.4}", s.error_rate()));
    println!("Bytes in         : {}", s.bytes_in);
    println!("Peak in-flight   : {}", s.peak_in_flight);
    if s.truncated > 0 {
        println!("Truncated body   : {}", s.truncated);
    }
    if s.dropped_tokens > 0 {
        println!("Dropped tokens   : {} (blocked {}us)", s.dropped_tokens, s.blocked_us);
    }
    if s.connect_count > 0 {
        println!("New connections  : {}", s.connect_count);
        println!("Avg connect      : {}ms", f2(s.connect_us_sum as f64 / s.connect_count as f64 / 1000.0));
    }
    if s.requests > 0 {
        println!("Avg latency      : {}ms", f2(s.latency.avg_us() / 1000.0));
        println!("Min latency      : {}us", s.latency.min_us);
        println!("Max latency      : {}us", s.latency.max_us);
        println!("P50 latency      : {}ms", f2(s.latency.percentile(50.0) / 1000.0));
        println!("P95 latency      : {}ms", f2(s.latency.percentile(95.0) / 1000.0));
        println!("P99 latency      : {}ms", f2(s.latency.percentile(99.0) / 1000.0));
        println!("P99.9 latency    : {}ms", f2(s.latency.percentile(99.9) / 1000.0));
        if s.ttfb.count > 0 {
            println!("TTFB min/avg/max : {}/{}/{}us", s.ttfb.min_us, f2(s.ttfb.avg_us()), s.ttfb.max_us);
            println!("TTFB P50/P99     : {}/{}ms", f2(s.ttfb.percentile(50.0) / 1000.0), f2(s.ttfb.percentile(99.0) / 1000.0));
        }
    }
    if !s.status_codes.is_empty() {
        println!("\nHTTP status codes:");
        let mut codes: Vec<u16> = s.status_codes.keys().copied().collect();
        codes.sort();
        for c in codes {
            println!("  {c}: {}", s.status_codes[&c]);
        }
    }
    if !s.errors.is_empty() {
        println!("\nErrors:");
        let mut errs: Vec<String> = s.errors.keys().cloned().collect();
        errs.sort();
        for e in errs {
            println!("  {e}: {}", s.errors[&e]);
        }
    }
    println!("Breaker (open/half/closed): {open}/{half}/{closed}");
    println!("============================================================");
}

fn print_json(s: &Stats, elapsed: f64, open: usize, half: usize, closed: usize) {
    let rps = s.requests as f64 / elapsed.max(1e-6);
    let status: Vec<String> = s
        .status_codes
        .iter()
        .map(|(k, v)| format!("\"{k}\":{v}"))
        .collect();
    let errors: Vec<String> = s
        .errors
        .iter()
        .map(|(k, v)| format!("\"{}\":{}", k, v))
        .collect();
    let per_ep: Vec<String> = s
        .per_endpoint
        .iter()
        .map(|(k, v)| format!("\"ep{k}\":{v}"))
        .collect();
    let json = format!(
        r#"{{"requests":{},"success":{},"failed":{},"rps":{:.2},"error_rate":{:.6},"bytes_in":{},"peak_in_flight":{},"truncated":{},"dropped_tokens":{},"connect_count":{},"p50_ms":{:.2},"p95_ms":{:.2},"p99_ms":{:.2},"p999_ms":{:.2},"ttfb_p50_ms":{:.2},"ttfb_p99_ms":{:.2},"min_us":{},"max_us":{},"status":{{{}}},"errors":{{{}}},"per_endpoint":{{{}}},"breaker_open":{},"breaker_half":{},"breaker_closed":{}}}"#,
        s.requests, s.success, s.failed, rps, s.error_rate(), s.bytes_in, s.peak_in_flight,
        s.truncated, s.dropped_tokens, s.connect_count,
        s.latency.percentile(50.0) / 1000.0, s.latency.percentile(95.0) / 1000.0,
        s.latency.percentile(99.0) / 1000.0, s.latency.percentile(99.9) / 1000.0,
        s.ttfb.percentile(50.0) / 1000.0, s.ttfb.percentile(99.0) / 1000.0,
        s.latency.min_us, s.latency.max_us,
        status.join(","), errors.join(","), per_ep.join(","),
        open, half, closed
    );
    println!("{json}");
}

// Install a SIGINT handler: first Ctrl+C sets `stop`; second aborts.
// Zero-dep: FFI to libc::signal (std already links libc on Unix).
#[cfg(unix)]
mod sig {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    static SIG_STOP: AtomicUsize = AtomicUsize::new(0);
    static SIGINT_COUNT: AtomicUsize = AtomicUsize::new(0);

    extern "C" {
        fn signal(signum: i32, handler: usize) -> usize;
    }

    // SAFETY: async-signal-safe; only touches atomics and aborts on second.
    unsafe extern "C" fn on_sig(_sig: i32) {
        SIGINT_COUNT.fetch_add(1, Ordering::Relaxed);
        if SIGINT_COUNT.load(Ordering::Relaxed) >= 2 {
            std::process::abort();
        }
        let s = SIG_STOP.load(Ordering::Relaxed);
        if s != 0 {
            unsafe { &*(s as *const AtomicBool) }.store(true, Ordering::Relaxed);
        }
    }

    pub fn install(stop: &Arc<AtomicBool>) {
        const SIGINT: i32 = 2;
        SIG_STOP.store(Arc::as_ptr(stop) as usize, Ordering::Relaxed);
        unsafe {
            signal(SIGINT, on_sig as usize);
        }
    }
}

#[cfg(unix)]
fn install_sigint(stop: &Arc<AtomicBool>) {
    sig::install(stop);
}

#[cfg(not(unix))]
fn install_sigint(_stop: &Arc<AtomicBool>) {}
"""

# [V33] Native multi-protocol Rust data plane. Current Rust protocol dependencies: hyper 1.11, h3 0.0.8, h3-quinn 0.0.10, quinn 0.11.11, tokio-tungstenite 0.30.

_RUST_NATIVE_CARGO_TOML = '[package]\nname = "termux_stress_native"\nversion = "37.0.0"\nedition = "2021"\n\n[profile.release]\nopt-level = 3\nlto = "thin"\ncodegen-units = 1\npanic = "abort"\nstrip = "symbols"\n\n[dependencies]\ntokio = { version = "1", features = ["rt", "rt-multi-thread", "macros", "net", "time", "sync"] }\nhyper = { version = "1.11", default-features = false, features = ["client", "http1", "http2"] }\nhyper-util = { version = "0.1.20", features = ["client-legacy", "http1", "http2", "tokio", "service"] }\nhyper-rustls = { version = "0.27.9", default-features = false, features = ["http1", "http2", "native-tokio", "ring", "tls12"] }\nhttp-body-util = "0.1"\nbytes = "1"\nfutures-util = "0.3"\ntokio-tungstenite = { version = "0.30", default-features = false, features = ["connect", "handshake", "stream", "rustls-tls-native-roots"] }\nh3 = "0.0.8"\nh3-quinn = "0.0.10"\nquinn = "0.11.11"\nserde_json = "1"\nurl = "2"\nrustls = "0.23"\nrustls-native-certs = "0.8"\nlibc = "0.2"\n'
_RUST_NATIVE_SRC = 'use bytes::{Buf, Bytes};\nuse futures_util::{SinkExt, StreamExt};\nuse futures_util::stream::FuturesUnordered;\nuse http_body_util::{BodyExt, Full};\nuse hyper::{Method, Request, Uri, header::{HeaderName, HeaderValue}};\nuse hyper_rustls::HttpsConnectorBuilder;\nuse hyper_util::client::legacy::Client;\nuse hyper_util::rt::{TokioExecutor, TokioTimer};\nuse serde_json::json;\nuse std::env;\nuse std::net::{SocketAddr, ToSocketAddrs};\nuse std::sync::Arc;\nuse std::sync::atomic::{AtomicBool, Ordering};\nuse std::time::{Duration, Instant};\nuse tokio::time::{sleep, sleep_until, timeout};\nuse tokio_tungstenite::{connect_async_with_config, tungstenite::{Message, protocol::WebSocketConfig}};\nuse url::Url;\n\n#[derive(Clone, Copy, Debug, PartialEq, Eq)]\nenum Proto { H1, H2, H3, WS }\n\n#[derive(Clone, Copy, Debug, PartialEq, Eq)]\nenum BodyMode { Count, Discard }\n\n#[derive(Clone, Copy, Debug, PartialEq, Eq)]\nenum WsMode { Send, Echo }\n\n#[derive(Clone)]\nstruct HeaderKV { name: HeaderName, value: HeaderValue }\n\n#[derive(Clone)]\nstruct Cfg {\n    protocol: Proto,\n    target: String,\n    path: String,\n    method: String,\n    body: Bytes,\n    headers: Arc<Vec<HeaderKV>>,\n    workers: usize,\n    duration: f64,\n    timeout: f64,\n    rps: f64,\n    burst_window_ms: u64,\n    max_in_flight: usize,\n    body_mode: BodyMode,\n    h3_conns: usize,\n    ws_mode: WsMode,\n    keepalive: bool,\n    benchmark_single_core: bool,\n    runtime_threads: usize,\n    cpu_core: Option<usize>,\n    rps_burst: f64,\n    rate_mode: String,\n    arrival_queue: usize,\n    arrival_drop_policy: String,\n    ramp_up: f64,\n}\n\nfn arg(a:&[String],k:&str,d:&str)->String {\n    let mut i=0usize;\n    while i<a.len(){ if a[i]==k && i+1<a.len(){ return a[i+1].clone(); } i+=1; }\n    d.to_string()\n}\nfn has_flag(a:&[String],k:&str)->bool { a.iter().any(|x| x==k) }\nfn f64arg(a:&[String],k:&str,d:f64)->f64 { arg(a,k,&d.to_string()).parse().unwrap_or(d) }\nfn usizearg(a:&[String],k:&str,d:usize)->usize { arg(a,k,&d.to_string()).parse().unwrap_or(d) }\nfn parse_proto(s:&str)->Result<Proto,String>{match s{"h1"=>Ok(Proto::H1),"h2"=>Ok(Proto::H2),"h3"=>Ok(Proto::H3),"ws"=>Ok(Proto::WS),_=>Err(format!("bad protocol {s}"))}}\nfn make_uri(target:&str,path:&str)->Result<Uri,String>{let mut b=target.trim_end_matches(\'/\').to_string();let p=if path.starts_with(\'/\') {path.to_string()} else {format!("/{path}")};b.push_str(&p);b.parse().map_err(|e|format!("bad uri: {e}"))}\n\n#[derive(Clone, Copy)]\nstruct Histogram { buckets:[u64;256] }\nimpl Default for Histogram { fn default()->Self{Self{buckets:[0u64;256]}} }\nimpl Histogram {\n    fn add(&mut self,us:u64){\n        let u=us.max(1);\n        let msb=63usize.saturating_sub(u.leading_zeros() as usize);\n        let sub=if msb>=2 {((u>>(msb-2))&3) as usize} else {((u<<(2-msb))&3) as usize};\n        let idx=(msb*4+sub).min(255);\n        self.buckets[idx]=self.buckets[idx].saturating_add(1);\n    }\n    fn merge(&mut self,other:&Self){for i in 0..256{self.buckets[i]=self.buckets[i].saturating_add(other.buckets[i]);}}\n    fn percentile(&self,p:f64,total:u64)->u64{\n        if total==0{return 0}\n        let rank=((total as f64)*p).ceil().max(1.0) as u64;\n        let mut seen=0u64;\n        for i in 0..256{seen=seen.saturating_add(self.buckets[i]);if seen>=rank{let msb=i/4;let sub=i%4;if msb==0{return 1};let base=1u64<<msb;return base.saturating_add(base.saturating_mul(sub as u64)/4);}}\n        0\n    }\n}\n\n#[derive(Default)]\nstruct WorkerMetrics {\n    attempts:u64, completed:u64, success:u64, failed:u64,\n    bytes_in:u64, bytes_out:u64,\n    connect_errors:u64, timeouts:u64, protocol_errors:u64,\n    latency_us_sum:u64, latency_us_max:u64,\n    histogram:Histogram,\n    ws_messages:u64, ws_handshakes:u64, ws_echo_success:u64, status_counts:[u64;600],\n}\nimpl WorkerMetrics {\n    fn observe(&mut self, success:bool, us:u64){\n        self.completed=self.completed.saturating_add(1);\n        if success{self.success=self.success.saturating_add(1)}else{self.failed=self.failed.saturating_add(1)}\n        self.latency_us_sum=self.latency_us_sum.saturating_add(us);self.latency_us_max=self.latency_us_max.max(us);self.histogram.add(us);\n    }\n    fn merge(&mut self,o:Self){\n        self.attempts+=o.attempts;self.completed+=o.completed;self.success+=o.success;self.failed+=o.failed;self.bytes_in+=o.bytes_in;self.bytes_out+=o.bytes_out;self.connect_errors+=o.connect_errors;self.timeouts+=o.timeouts;self.protocol_errors+=o.protocol_errors;self.latency_us_sum+=o.latency_us_sum;self.latency_us_max=self.latency_us_max.max(o.latency_us_max);self.histogram.merge(&o.histogram);self.ws_messages+=o.ws_messages;self.ws_handshakes+=o.ws_handshakes;self.ws_echo_success+=o.ws_echo_success;for i in 0..600{self.status_counts[i]=self.status_counts[i].saturating_add(o.status_counts[i]);}\n    }\n}\n\n#[derive(Debug)]\nstruct Outcome { ok:bool, latency_us:u64, bytes_in:u64, status:u16, timeout:bool, connect_error:bool, protocol_error:bool }\nfn parse_headers(a:&[String])->Result<Vec<HeaderKV>,String>{let mut out=Vec::new();let mut i=0usize;while i<a.len(){if a[i]=="--header"&&i+1<a.len(){let raw=&a[i+1];let (n,v)=raw.split_once(\':\').ok_or_else(||format!("invalid header: {raw}"))?;let name=HeaderName::from_bytes(n.trim().as_bytes()).map_err(|_|format!("invalid header name: {n}"))?;let value=HeaderValue::from_str(v.trim()).map_err(|_|format!("invalid header value for {n}"))?;out.push(HeaderKV{name,value});i+=2}else{i+=1}}Ok(out)}\nfn apply_headers<B>(mut req:Request<B>, headers:&[HeaderKV])->Request<B>{for h in headers{req.headers_mut().append(h.name.clone(),h.value.clone());}req}\n\nasync fn consume_hyper_body(mut body:hyper::body::Incoming, count:bool)->Result<u64,hyper::Error>{\n    let mut total=0u64;\n    while let Some(frame)=body.frame().await{\n        let frame=frame?;\n        if count { if let Some(data)=frame.data_ref(){ total=total.saturating_add(data.len() as u64); } }\n    }\n    Ok(total)\n}\n\nasync fn run_hyper_worker(c:Cfg,_worker_id:usize)->WorkerMetrics{\n    let mut out=WorkerMetrics::default();\n    let connector=match HttpsConnectorBuilder::new().with_native_roots(){Ok(b)=>b.https_or_http().enable_http1().enable_http2().build(),Err(_)=>{return WorkerMetrics::default()}};\n    let mut builder=Client::builder(TokioExecutor::new());\n    builder.pool_timer(TokioTimer::new()).pool_idle_timeout(Duration::from_secs(30));\n    if !c.keepalive{builder.pool_max_idle_per_host(0);}\n    if c.protocol==Proto::H2{builder.http2_only(true);builder.http2_max_concurrent_streams(1024);}\n    let client=builder.build(connector);\n    let uri=match make_uri(&c.target,&c.path){Ok(v)=>v,Err(_)=>return out};\n    let method=c.method.parse::<Method>().unwrap_or(Method::GET);\n    let start=Instant::now();\n    let end=start+Duration::from_secs_f64(c.duration.max(0.001));\n    let window=Duration::from_millis(c.burst_window_ms.clamp(1,100));\n    let base_per_rps=if c.rps>0.0 {c.rps/(c.workers.max(1) as f64)} else {0.0};\n    let batch_cap=if base_per_rps>0.0{((base_per_rps*window.as_secs_f64()).ceil() as usize).clamp(1,32768)}else{1024};let burst_cap=if c.rps_burst>0.0{(c.rps_burst.max(1.0)*window.as_secs_f64()).ceil() as usize}else{usize::MAX};\n    let mut scheduled=0u64;\n    let mut next_tick=start+window;\n    let queue_cap=if c.rate_mode=="arrival"{c.arrival_queue.max(1)}else{usize::MAX};\n    let worker_flight=(c.max_in_flight.max(1).saturating_add(c.workers.max(1)-1)/c.workers.max(1)).min(queue_cap);let max_flight=worker_flight;\n    let mut inflight=FuturesUnordered::new();\n    loop{\n        let now=Instant::now();\n        if now<end{\n            let available=max_flight.saturating_sub(inflight.len());\n            if available>0{\n                let want=if base_per_rps>0.0{let progress=if c.ramp_up>0.0{((now-start).as_secs_f64()/c.ramp_up).clamp(0.0,1.0)}else{1.0};let effective_rps=base_per_rps*progress;let desired=((now-start).as_secs_f64()*effective_rps).floor() as u64;desired.saturating_sub(scheduled).min(available as u64).min(batch_cap.min(burst_cap.max(1usize)) as u64)}else{available.min(1024) as u64};\n                for _ in 0..want{\n                    out.attempts=out.attempts.saturating_add(1);scheduled=scheduled.saturating_add(1);\n                    let cc=client.clone();let u=uri.clone();let mm=method.clone();let body=c.body.clone();let headers=c.headers.clone();let timeout_s=c.timeout.max(0.001);let skip=matches!(c.body_mode,BodyMode::Discard);let protocol=c.protocol;\n                    inflight.push(async move{\n                        let st=Instant::now();\n                        let req0=Request::builder().method(mm).uri(u).body(Full::new(body));\n                        let req=match req0{Ok(r)=>apply_headers(r,&headers),Err(_)=>return Outcome{ok:false,latency_us:st.elapsed().as_micros() as u64,bytes_in:0,status:0,timeout:false,connect_error:false,protocol_error:true}};\n                        let res=timeout(Duration::from_secs_f64(timeout_s),cc.request(req)).await;\n                        match res{\n                            Ok(Ok(resp))=>{\n                                let status=resp.status();\n                                let body_bytes=if skip{consume_hyper_body(resp.into_body(),!skip).await.unwrap_or(0)}else{consume_hyper_body(resp.into_body(),true).await.unwrap_or(0)};\n                                Outcome{ok:status.as_u16()<400,latency_us:st.elapsed().as_micros() as u64,bytes_in:body_bytes,status:status.as_u16(),timeout:false,connect_error:false,protocol_error:false}\n                            }\n                            Ok(Err(e))=>Outcome{ok:false,latency_us:st.elapsed().as_micros() as u64,bytes_in:0,status:0,timeout:false,connect_error:e.is_connect(),protocol_error:false},\n                            Err(_)=>Outcome{ok:false,latency_us:st.elapsed().as_micros() as u64,bytes_in:0,status:0,timeout:true,connect_error:false,protocol_error:false},\n                        }\n                    });\n                }\n            }\n        }\n        if inflight.is_empty(){\n            if Instant::now()>=end{break}\n            if base_per_rps>0.0{sleep_until(next_tick.into()).await;next_tick+=window;} else {tokio::task::yield_now().await;}\n            continue\n        }\n        if base_per_rps>0.0 && Instant::now()<end{\n            tokio::select!{\n                r= inflight.next()=>{if let Some(o)=r{if o.timeout{out.timeouts+=1} if o.connect_error{out.connect_errors+=1} if o.protocol_error{out.protocol_errors+=1} out.bytes_in+=o.bytes_in;if (o.status as usize)<out.status_counts.len(){out.status_counts[o.status as usize]+=1;}out.observe(o.ok,o.latency_us);}},\n                _=sleep_until(next_tick.into())=>{while next_tick<=Instant::now(){next_tick+=window;}}\n            }\n        }else if let Some(o)=inflight.next().await{\n            if o.timeout{out.timeouts+=1}if o.connect_error{out.connect_errors+=1}if o.protocol_error{out.protocol_errors+=1}out.bytes_in+=o.bytes_in;if (o.status as usize)<out.status_counts.len(){out.status_counts[o.status as usize]+=1;}out.observe(o.ok,o.latency_us);\n        }\n    }\n    while let Some(o)=inflight.next().await{if o.timeout{out.timeouts+=1}if o.connect_error{out.connect_errors+=1}if o.protocol_error{out.protocol_errors+=1}out.bytes_in+=o.bytes_in;if (o.status as usize)<out.status_counts.len(){out.status_counts[o.status as usize]+=1;}out.observe(o.ok,o.latency_us);}\n    out.bytes_out=out.attempts.saturating_mul(c.body.len() as u64);\n    out\n}\n\nasync fn run_hyper(c:Cfg)->WorkerMetrics{let mut joins=tokio::task::JoinSet::new();for id in 0..c.workers.max(1){joins.spawn(run_hyper_worker(c.clone(),id));}let mut total=WorkerMetrics::default();while let Some(r)=joins.join_next().await{if let Ok(m)=r{total.merge(m)}}total}\n\nfn load_roots()->Result<rustls::RootCertStore,String>{let mut roots=rustls::RootCertStore::empty();let store=rustls_native_certs::load_native_certs().map_err(|e|e.to_string())?;for cert in store.certs{let _=roots.add(cert);}Ok(roots)}\n\nasync fn h3_connection(c:Cfg, worker_ids:Vec<usize>)->WorkerMetrics{\n    let mut total=WorkerMetrics::default();\n    let u=match Url::parse(&c.target){Ok(v)=>v,Err(_)=>return total};\n    let host=u.host_str().unwrap_or("localhost").to_string();let port=u.port().unwrap_or(443);\n    let addr=match (host.as_str(),port).to_socket_addrs(){Ok(mut it)=>match it.next(){Some(x)=>x,None=>{total.connect_errors+=1;return total}},Err(_)=>{total.connect_errors+=1;return total}};\n    let bind_addr=match "0.0.0.0:0".parse::<SocketAddr>(){Ok(x)=>x,Err(_)=>return total};\n    let mut ep=match quinn::Endpoint::client(bind_addr){Ok(x)=>x,Err(_)=>{total.connect_errors+=1;return total}};\n    let roots=match load_roots(){Ok(x)=>x,Err(_)=>{total.connect_errors+=1;return total}};\n    let mut tls=rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();tls.alpn_protocols=vec![b"h3".to_vec()];\n    let q=match quinn::crypto::rustls::QuicClientConfig::try_from(tls){Ok(x)=>x,Err(_)=>{total.connect_errors+=1;return total}};\n    ep.set_default_client_config(quinn::ClientConfig::new(Arc::new(q)));\n    let conn=match ep.connect(addr,&host){Ok(f)=>match f.await{Ok(v)=>v,Err(_)=>{total.connect_errors+=1;return total}},Err(_)=>{total.connect_errors+=1;return total}};\n    let h3c=h3_quinn::Connection::new(conn);\n    let (driver,send)=match h3::client::new(h3c).await{Ok(v)=>v,Err(_)=>{total.connect_errors+=1;return total}};\n    let driver_task=tokio::spawn(async move{let mut d=driver;let _=std::future::poll_fn(|cx|d.poll_close(cx)).await;});\n    let mut joins=tokio::task::JoinSet::new();\n    for id in worker_ids{joins.spawn(run_h3_worker(send.clone(),c.clone(),id));}\n    while let Some(r)=joins.join_next().await{if let Ok(m)=r{total.merge(m)}}\n    driver_task.abort();let _=driver_task.await;ep.wait_idle().await;total\n}\n\nasync fn run_h3_worker(send:h3::client::SendRequest<h3_quinn::Connection,Bytes>,c:Cfg,_worker_id:usize)->WorkerMetrics{\n    let mut out=WorkerMetrics::default();let uri=match make_uri(&c.target,&c.path){Ok(v)=>v,Err(_)=>return out};let start=Instant::now();let end=start+Duration::from_secs_f64(c.duration.max(0.001));let window=Duration::from_millis(c.burst_window_ms.clamp(1,100));let base_per_rps=if c.rps>0.0{c.rps/(c.workers.max(1) as f64)}else{0.0};let batch_cap=if base_per_rps>0.0{((base_per_rps*window.as_secs_f64()).ceil() as usize).clamp(1,32768)}else{1024};let queue_cap=if c.rate_mode=="arrival"{c.arrival_queue.max(1)}else{usize::MAX};let worker_flight=(c.max_in_flight.max(1).saturating_add(c.workers.max(1)-1)/c.workers.max(1)).min(queue_cap);let max_flight=worker_flight;let mut scheduled=0u64;let mut next_tick=start+window;let burst_cap=if c.rps_burst>0.0{(c.rps_burst.max(1.0)*window.as_secs_f64()).ceil() as usize}else{usize::MAX};let mut inflight=FuturesUnordered::new();\n    loop{\n        let now=Instant::now();\n        if now<end{let available=max_flight.saturating_sub(inflight.len());if available>0{let want=if base_per_rps>0.0{let progress=if c.ramp_up>0.0{((now-start).as_secs_f64()/c.ramp_up).clamp(0.0,1.0)}else{1.0};let effective_rps=base_per_rps*progress;let desired=((now-start).as_secs_f64()*effective_rps).floor() as u64;desired.saturating_sub(scheduled).min(available as u64).min(batch_cap.min(burst_cap.max(1usize)) as u64)}else{available.min(1024) as u64};for _ in 0..want{out.attempts+=1;scheduled+=1;let mut sr=send.clone();let cc=c.clone();let u=uri.clone();let headers=cc.headers.clone();let timeout_s=cc.timeout.max(0.001);let method=cc.method.clone();let body=cc.body.clone();inflight.push(async move{let st=Instant::now();let req0=Request::builder().method(method).uri(u).body(());let mut req=match req0{Ok(r)=>apply_headers(r,&headers),Err(_)=>return Outcome{ok:false,latency_us:st.elapsed().as_micros() as u64,bytes_in:0,status:0,timeout:false,connect_error:false,protocol_error:true}};let _=&mut req;let res=timeout(Duration::from_secs_f64(timeout_s),async{let mut stream=sr.send_request(req).await.map_err(|_|())?;if !body.is_empty(){stream.send_data(body.clone()).await.map_err(|_|())?;}stream.finish().await.map_err(|_|())?;let resp=stream.recv_response().await.map_err(|_|())?;let status_u=resp.status().as_u16();let mut bin=0u64;while let Some(chunk)=stream.recv_data().await.map_err(|_|())?{bin+=chunk.remaining() as u64;}Ok::<(bool,u64,u16),()>( (status_u<400, bin, status_u) )}).await;match res{Ok(Ok((ok,bin,status_u)))=>Outcome{ok,latency_us:st.elapsed().as_micros() as u64,bytes_in:bin,status:status_u,timeout:false,connect_error:false,protocol_error:false},Ok(Err(_))=>Outcome{ok:false,latency_us:st.elapsed().as_micros() as u64,bytes_in:0,status:0,timeout:false,connect_error:false,protocol_error:true},Err(_)=>Outcome{ok:false,latency_us:st.elapsed().as_micros() as u64,bytes_in:0,status:0,timeout:true,connect_error:false,protocol_error:false}}});}}\n        }\n        if inflight.is_empty(){if Instant::now()>=end{break}if base_per_rps>0.0{sleep_until(next_tick.into()).await;next_tick+=window}else{tokio::task::yield_now().await}continue;}\n        if base_per_rps>0.0&&Instant::now()<end{tokio::select!{r=inflight.next()=>{if let Some(o)=r{if o.timeout{out.timeouts+=1}if o.protocol_error{out.protocol_errors+=1}if (o.status as usize)<out.status_counts.len(){out.status_counts[o.status as usize]+=1;}out.observe(o.ok,o.latency_us)}},_=sleep_until(next_tick.into())=>{while next_tick<=Instant::now(){next_tick+=window;}}}}else if let Some(o)=inflight.next().await{if o.timeout{out.timeouts+=1}if o.protocol_error{out.protocol_errors+=1}if (o.status as usize)<out.status_counts.len(){out.status_counts[o.status as usize]+=1;}out.observe(o.ok,o.latency_us)}\n    }\n    while let Some(o)=inflight.next().await{if o.timeout{out.timeouts+=1}if o.protocol_error{out.protocol_errors+=1}if (o.status as usize)<out.status_counts.len(){out.status_counts[o.status as usize]+=1;}out.observe(o.ok,o.latency_us)}\n    out.bytes_out=out.attempts.saturating_mul(c.body.len() as u64);out\n}\n\nasync fn run_h3(c:Cfg)->WorkerMetrics{let n=c.h3_conns.max(1).min(c.workers.max(1));let mut groups=vec![Vec::new();n];for id in 0..c.workers.max(1){groups[id%n].push(id)}let mut joins=tokio::task::JoinSet::new();for g in groups{if !g.is_empty(){joins.spawn(h3_connection(c.clone(),g));}}let mut total=WorkerMetrics::default();while let Some(r)=joins.join_next().await{if let Ok(m)=r{total.merge(m)}}total}\n\nasync fn run_ws_worker(c:Cfg,_worker_id:usize)->WorkerMetrics{\n    let mut out=WorkerMetrics::default();\n    let u=if c.target.starts_with("ws://")||c.target.starts_with("wss://"){format!("{}{}",c.target.trim_end_matches(\'/\'),if c.path.starts_with(\'/\'){c.path.clone()}else{format!("/{}",c.path)})}else{return out};\n    let cfg=WebSocketConfig::default().max_message_size(Some(16*1024*1024)).max_frame_size(Some(16*1024*1024));\n    let (mut ws,_resp)=match timeout(Duration::from_secs_f64(c.timeout.max(0.1)),connect_async_with_config(&u,Some(cfg),false)).await{Ok(Ok(v))=>v,Ok(Err(_))|Err(_)=>{out.connect_errors+=1;return out}};\n    out.ws_handshakes+=1;\n    let start=Instant::now();let end=start+Duration::from_secs_f64(c.duration.max(0.001));let window=Duration::from_millis(c.burst_window_ms.clamp(1,100));let base_per_rps=if c.rps>0.0{c.rps/(c.workers.max(1) as f64)}else{0.0};let batch_cap=if base_per_rps>0.0{((base_per_rps*window.as_secs_f64()).ceil() as usize).clamp(1,32768)}else{1};let burst_cap=if c.rps_burst>0.0{(c.rps_burst.max(1.0)*window.as_secs_f64()).ceil() as usize}else{usize::MAX};let mut scheduled=0u64;let mut next_tick=start+window;\n    while Instant::now()<end{\n        let now=Instant::now();let budget=if base_per_rps>0.0{let progress=if c.ramp_up>0.0{((now-start).as_secs_f64()/c.ramp_up).clamp(0.0,1.0)}else{1.0};let effective_rps=base_per_rps*progress;let desired=((now-start).as_secs_f64()*effective_rps).floor() as u64;desired.saturating_sub(scheduled).min(batch_cap.min(burst_cap) as u64)}else{1};\n        if budget==0{sleep_until(next_tick.into()).await;while next_tick<=Instant::now(){next_tick+=window;}continue;}\n        for _ in 0..budget{\n            out.attempts+=1;scheduled+=1;let st=Instant::now();let msg=if c.body.is_empty(){Message::Ping(Bytes::new())}else{Message::Binary(c.body.clone())};let send_res=timeout(Duration::from_secs_f64(c.timeout.max(0.1)),ws.send(msg)).await;match send_res{Ok(Ok(_))=>{if c.ws_mode==WsMode::Echo{let echoed=timeout(Duration::from_secs_f64(c.timeout.max(0.1)),ws.next()).await;match echoed{Ok(Some(Ok(_)))=>{out.ws_echo_success+=1;out.ws_messages+=1;out.observe(true,st.elapsed().as_micros() as u64)},_=>{out.timeouts+=1;out.observe(false,st.elapsed().as_micros() as u64)}}}else{out.ws_messages+=1;out.observe(true,st.elapsed().as_micros() as u64)}},Ok(Err(_))=>{out.protocol_errors+=1;out.observe(false,st.elapsed().as_micros() as u64);return out},Err(_)=>{out.timeouts+=1;out.observe(false,st.elapsed().as_micros() as u64);return out}}\n        }\n        next_tick+=window;if base_per_rps>0.0 && Instant::now()<next_tick{sleep_until(next_tick.into()).await}else{next_tick=Instant::now()}\n    }\n    let _=ws.close(None).await;out.bytes_out=out.attempts.saturating_mul(c.body.len() as u64);out\n}\nasync fn run_ws(c:Cfg)->WorkerMetrics{let mut joins=tokio::task::JoinSet::new();for id in 0..c.workers.max(1){joins.spawn(run_ws_worker(c.clone(),id));}let mut total=WorkerMetrics::default();while let Some(r)=joins.join_next().await{if let Ok(m)=r{total.merge(m)}}total}\n\nfn maybe_pin_cpu(core:Option<usize>)->bool{\n    let Some(core)=core else{return false};\n    #[cfg(any(target_os="linux",target_os="android"))]\n    unsafe{\n        let mut set:libc::cpu_set_t=std::mem::zeroed();\n        libc::CPU_ZERO(&mut set);libc::CPU_SET(core,&mut set);\n        return libc::sched_setaffinity(0,std::mem::size_of::<libc::cpu_set_t>(),&set)==0;\n    }\n    #[cfg(not(any(target_os="linux",target_os="android")))]{let _=core;false}\n}\n\nfn build_runtime(single:bool,threads:usize)->Result<tokio::runtime::Runtime,String>{\n    let r=if single{tokio::runtime::Builder::new_current_thread().enable_all().build()}\n    else{tokio::runtime::Builder::new_multi_thread().enable_all().worker_threads(threads.max(1)).max_blocking_threads(threads.max(1)).build()};\n    r.map_err(|e|format!("runtime init failed: {e}"))\n}\n\nfn histogram_json(h:&Histogram,total:u64)->serde_json::Value{json!({"p50_us":h.percentile(0.50,total),"p95_us":h.percentile(0.95,total),"p99_us":h.percentile(0.99,total),"p999_us":h.percentile(0.999,total)})}\n\nasync fn async_main(a:Vec<String>){\n    let p=match parse_proto(&arg(&a,"--protocol","h1")){Ok(v)=>v,Err(e)=>{eprintln!("{e}");std::process::exit(2)}};\n    if has_flag(&a,"--version"){println!("37.0.0");return}\n    let single=arg(&a,"--benchmark","off")=="single-core";\n    let workers=usizearg(&a,"--workers",1).max(1);\n    let body_mode=if arg(&a,"--body-mode","count")=="discard"{BodyMode::Discard}else{BodyMode::Count};\n    let ws_mode=if arg(&a,"--ws-mode","send")=="echo"{WsMode::Echo}else{WsMode::Send};\n    let body=Bytes::from(arg(&a,"--body","").into_bytes());\n    if arg(&a,"--method","GET").parse::<Method>().is_err(){eprintln!("invalid HTTP method");std::process::exit(2);}\n    let hdr=match parse_headers(&a){Ok(v)=>Arc::new(v),Err(e)=>{eprintln!("{e}");std::process::exit(2)}};\n    let runtime_threads=usizearg(&a,"--runtime-threads",if single{1}else{workers.min(8).max(1)});\n    let cpu_core=if a.iter().any(|x|x=="--cpu-core"){Some(usizearg(&a,"--cpu-core",0))}else{None};\n    let affinity=maybe_pin_cpu(cpu_core);\n    let c=Cfg{protocol:p,target:arg(&a,"--target","http://127.0.0.1:8080"),path:arg(&a,"--path","/"),method:arg(&a,"--method","GET"),body,headers:hdr,workers,duration:f64arg(&a,"--duration",30.0),timeout:f64arg(&a,"--timeout",10.0),rps:f64arg(&a,"--rps",0.0),burst_window_ms:usizearg(&a,"--burst-window-ms",10) as u64,max_in_flight:usizearg(&a,"--max-in-flight",16384),body_mode,h3_conns:usizearg(&a,"--h3-conns",1),ws_mode,keepalive:!has_flag(&a,"--no-keepalive"),benchmark_single_core:single,runtime_threads,cpu_core, rps_burst:f64arg(&a,"--rps-burst",0.0), rate_mode:arg(&a,"--rate-mode","worker"), arrival_queue:usizearg(&a,"--arrival-queue",16384).max(1), arrival_drop_policy:arg(&a,"--arrival-drop-policy","drop"), ramp_up:f64arg(&a,"--ramp-up",0.0).max(0.0)};\n    let started=Instant::now();\n    let result=match p{Proto::H1|Proto::H2=>run_hyper(c.clone()).await,Proto::H3=>run_h3(c.clone()).await,Proto::WS=>run_ws(c.clone()).await};\n    let secs=started.elapsed().as_secs_f64().max(1e-9);\n    let avg=(result.latency_us_sum as f64)/(result.completed.max(1) as f64);\n    println!("{}",json!({\n        "version":"37.0.0","protocol":match p{Proto::H1=>"h1",Proto::H2=>"h2",Proto::H3=>"h3",Proto::WS=>"ws"},\n        "attempts":result.attempts,"completed":result.completed,"requests":result.completed,\n        "success":result.success,"failed":result.failed,\n        "rps":result.completed as f64/secs,"attempt_rps":result.attempts as f64/secs,\n        "error_rate":result.failed as f64/(result.completed.max(1) as f64),\n        "bytes_in":result.bytes_in,"bytes_out":result.bytes_out,\n        "connect_errors":result.connect_errors,"timeouts":result.timeouts,"protocol_errors":result.protocol_errors,\n        "ws_messages":result.ws_messages,"ws_handshakes":result.ws_handshakes,"ws_echo_success":result.ws_echo_success,\n        "status_counts":result.status_counts.iter().enumerate().filter(|(_,v)|**v>0).map(|(k,v)|(k.to_string(),*v)).collect::<std::collections::BTreeMap<_,_>>(),\n        "avg_latency_ms":avg/1000.0,"max_latency_ms":result.latency_us_max as f64/1000.0,\n        "percentiles":histogram_json(&result.histogram,result.completed),"elapsed_s":secs,\n        "single_core_target_rps":1000000u64,"extreme_validation_target_rps":100000000000u64,"benchmark_target_only":true,"single_core_runtime":single,\n        "runtime_threads":c.runtime_threads,"cpu_core":c.cpu_core,"cpu_affinity_applied":affinity,\n        "workers":c.workers,"h3_conns":c.h3_conns,"max_in_flight":c.max_in_flight,"arrival_queue":c.arrival_queue,"rate_mode":c.rate_mode,"rps_burst":c.rps_burst,"ramp_up":c.ramp_up,\n        "metrics_semantics":{"bytes_in":"response body bytes when count mode; zero in discard mode","bytes_out":"request body bytes","attempts":"scheduled operations","completed":"finished operations"},"compatibility":{"all_cli_options_accepted":true,"protocol_inapplicable_options":"accepted without hard compatibility failure; effective semantics are reported; TCP-specific knobs are neutralized outside raw engine; security-sensitive interference is never synthesized by native Rust"}\n    }));\n}\n\nfn main(){\n    let a:Vec<String>=env::args().collect();\n    if a.iter().any(|x|x=="--version"){println!("37.0.0");return}\n    let single=arg(&a,"--benchmark","off")=="single-core";\n    let threads=usizearg(&a,"--runtime-threads",if single{1}else{usizearg(&a,"--workers",1).min(8).max(1)});\n    match build_runtime(single,threads){Ok(rt)=>rt.block_on(async_main(a)),Err(e)=>{eprintln!("{e}");std::process::exit(2)}}\n}\n'

_RUST_BIN_CACHE = None
_RUST_REPORT = None

def _rustc_find():
    import shutil, os, glob
    p = shutil.which("rustc")
    if p:
        return p
    for pat in (os.path.expanduser("~/.toolchain/rust/rustc-*/rustc/bin/rustc"),
               os.path.expanduser("/workspace/.toolchain/rust/rustc-*/rustc/bin/rustc")):
        for c in glob.glob(pat):
            if os.path.isfile(c) and os.access(c, os.X_OK):
                return c
    return None

def _cargo_find():
    p = shutil.which("cargo")
    if p:
        return p
    for cand in (os.path.expanduser("~/.cargo/bin/cargo"),
                 os.path.expanduser("~/.toolchain/rust/cargo/bin/cargo"),
                 "/workspace/.toolchain/rust/cargo/bin/cargo"):
        if os.path.isfile(cand) and os.access(cand, os.X_OK):
            return cand
    return None

def _ensure_rust_native_binary():
    import tempfile, subprocess, hashlib, os
    root=os.path.join(tempfile.gettempdir(),"termux_stress_native_v36")
    h=hashlib.sha256((_RUST_NATIVE_CARGO_TOML+_RUST_NATIVE_SRC).encode()).hexdigest()[:16]
    project=os.path.join(root,h)
    target=os.path.join(project,"target","release","termux_stress_native")
    if os.name=="nt": target += ".exe"
    if os.path.exists(target) and os.access(target, os.X_OK): return target
    cargo=_cargo_find()
    if not cargo: raise RuntimeError("cargo not found; native Rust H1/H2/H3/WS backend unavailable")
    os.makedirs(os.path.join(project,"src"),exist_ok=True)
    Path(os.path.join(project,"Cargo.toml")).write_text(_RUST_NATIVE_CARGO_TOML,encoding="utf-8")
    Path(os.path.join(project,"src","main.rs")).write_text(_RUST_NATIVE_SRC,encoding="utf-8")
    r=subprocess.run([cargo,"build","--release","--manifest-path",os.path.join(project,"Cargo.toml")],capture_output=True,text=True,timeout=900,env={**os.environ,"CARGO_TERM_COLOR":"never"})
    if r.returncode!=0: raise RuntimeError("native Rust Cargo build failed: "+(r.stderr or r.stdout)[-2400:])
    if not os.path.exists(target): raise RuntimeError("native Rust binary missing after cargo build")
    os.chmod(target,0o755)
    return target

def _ensure_rust_binary():
    global _RUST_BIN_CACHE
    import os, tempfile, subprocess, hashlib, time
    if _RUST_BIN_CACHE and os.path.exists(_RUST_BIN_CACHE): return _RUST_BIN_CACHE
    rustc=_rustc_find()
    if not rustc: raise RuntimeError("rustc not found; Rust engine unavailable (use a Python engine)")
    src_hash=hashlib.sha256(RUST_CORE_SRC.encode()).hexdigest()[:16]
    tmpdir=tempfile.gettempdir();src_path=os.path.join(tmpdir,"tc_stress_core_%s.rs"%src_hash);bin_path=os.path.join(tmpdir,"tc_stress_core_%s.bin"%src_hash)
    if not (os.path.exists(bin_path) and os.access(bin_path,os.X_OK)):
        if not os.path.exists(src_path): Path(src_path).write_text(RUST_CORE_SRC,encoding="utf-8")
        t0=time.time();last_err=None
        for cmd in ([rustc,"-O","--edition=2021","-C","lto","-C","panic=abort","-C","link-arg=-fuse-ld=bfd",src_path,"-o",bin_path], [rustc,"-O","--edition=2021",src_path,"-o",bin_path]):
            try:
                r=subprocess.run(cmd,capture_output=True,text=True,timeout=300)
                if r.returncode==0 and os.path.exists(bin_path): break
                last_err=r.stderr[-800:]
            except Exception as e: last_err=str(e)
        else: raise RuntimeError("rustc failed: "+str(last_err))
        os.chmod(bin_path,0o755);_info("[rust] compiled embedded core in %.1fs"%(time.time()-t0))
    _RUST_BIN_CACHE=bin_path;return bin_path

def _rust_scenario_supported(args, norm_targets):
    """Universal engine contract gate.

    All CLI parameters remain accepted across engines. Protocol-specific options
    are normalized by the control plane; parameters that cannot have wire-level
    meaning for a selected protocol are recorded as neutralized semantics rather
    than causing a compatibility error. Security-sensitive TCP interference is
    never synthesized by the native Rust data plane.
    """
    unsupported = []
    rust_proto=getattr(args,"rust_protocol","auto")
    if rust_proto=="auto":
        if any(urlparse(t).scheme.lower() in ("ws","wss") for t in norm_targets): rust_proto="ws"
        elif getattr(args,"engine","rust")=="h3": rust_proto="h3"
        elif getattr(args,"engine","rust")=="h2": rust_proto="h2"
        else: rust_proto="h1"
    if len(norm_targets) > 1:
        unsupported.append("multiple targets are normalized to one target per native Rust process")
    for t in norm_targets:
        parsed=urlparse(t if "://" in t else f"{getattr(args,'default_scheme','http')}://{t}")
        scheme=parsed.scheme.lower()
        if rust_proto == "h1" and scheme not in ("http","https"):
            unsupported.append("rust h1 supports http/https only")
        if rust_proto == "h2" and scheme not in ("http","https"):
            unsupported.append("rust h2 supports http/https only")
        if rust_proto == "h3" and scheme not in ("https","h3"):
            unsupported.append("rust h3 supports https only")
        if rust_proto == "ws" and scheme not in ("ws","wss"):
            unsupported.append("rust ws supports ws/wss only")
    if getattr(args,"duration",0) <= 0:
        unsupported.append("duration<=0")
    # These are compatibility notes, not hard failures. They are surfaced in the
    # effective configuration/report and do not silently change protocol choice.
    return True, "; ".join(dict.fromkeys(unsupported))

def _run_rust_engine(args, targets):
    """Run v37 native Rust multi-protocol data plane with explicit semantics."""
    import subprocess, json
    rust_proto=getattr(args,"rust_protocol","auto")
    if rust_proto=="auto":
        schemes={urlparse(t).scheme.lower() for t in targets}
        if any(x in ("ws","wss") for x in schemes): rust_proto="ws"
        elif getattr(args,"engine","rust")=="h3": rust_proto="h3"
        elif getattr(args,"engine","rust")=="h2": rust_proto="h2"
        else: rust_proto="h1"
    bin_path=_ensure_rust_native_binary();single=getattr(args,"benchmark","off")=="single-core"
    rust_workers=1 if single else max(1,args.workers);runtime_threads=1 if single else (max(1,int(getattr(args,"rust_runtime_threads",0))) if getattr(args,"rust_runtime_threads",0)>0 else max(1,min(args.workers,8)))
    cmd=[bin_path,"--protocol",rust_proto,"--target",targets[0],"--workers",str(rust_workers),"--runtime-threads",str(runtime_threads),"--duration",str(float(args.duration or 30.0)),"--timeout",str(float(args.timeout or 10.0)),"--burst-window-ms",str(int(getattr(args,"rust_burst_window_ms",10))),"--max-in-flight",str(max(1,min(1000000,int(getattr(args,"max_in_flight",16384))))),"--h3-conns",str(max(1,int(getattr(args,"h3_conns",1))))]
    if getattr(args,"rps",0)>0: cmd += ["--rps",str(args.rps)]
    if getattr(args,"rps_burst",None) is not None: cmd += ["--rps-burst",str(float(args.rps_burst))]
    if getattr(args,"rate_mode",None): cmd += ["--rate-mode",str(args.rate_mode)]
    if getattr(args,"arrival_queue",None) is not None: cmd += ["--arrival-queue",str(max(1,int(args.arrival_queue)))]
    if getattr(args,"arrival_drop_policy",None): cmd += ["--arrival-drop-policy",str(args.arrival_drop_policy)]
    if getattr(args,"ramp_up",None) is not None: cmd += ["--ramp-up",str(float(args.ramp_up))]
    if single: cmd += ["--benchmark","single-core"]
    if getattr(args,"path","/")!="/": cmd += ["--path",args.path]
    if getattr(args,"method","GET")!="GET": cmd += ["--method",args.method]
    if getattr(args,"body",None) is not None: cmd += ["--body",args.body]
    cmd += ["--body-mode","discard" if (getattr(args,"skip_body",False) or getattr(args,"body_mode","count")=="discard") else "count"]
    if getattr(args,"no_keepalive",False): cmd += ["--no-keepalive"]
    if getattr(args,"max_conns_per_ep",0): cmd += ["--max-conns-per-ep",str(max(1,int(args.max_conns_per_ep)))]
    if getattr(args,"h2_conns_per_ep",0): cmd += ["--h2-conns-per-ep",str(max(1,int(args.h2_conns_per_ep)))]
    if getattr(args,"rps_burst",None) is not None: cmd += ["--rps-burst",str(float(args.rps_burst))]
    if getattr(args,"arrival_queue",0): cmd += ["--arrival-queue",str(max(1,int(args.arrival_queue)))]
    if getattr(args,"arrival_drop_policy",None): cmd += ["--arrival-drop-policy",str(args.arrival_drop_policy)]
    if getattr(args,"ramp_up",0): cmd += ["--ramp-up",str(float(args.ramp_up))]
    if getattr(args,"randomize",False): cmd += ["--randomize"]
    if getattr(args,"insecure",False): cmd += ["--insecure"]
    if getattr(args,"prefer_ips",None): cmd += ["--prefer-ips",str(args.prefer_ips)]
    if getattr(args,"data_file",None): cmd += ["--data-file",str(args.data_file)]
    if getattr(args,"format",None): cmd += ["--format",str(args.format)]
    if getattr(args,"cpu_core",None) is not None: cmd += ["--cpu-core",str(args.cpu_core)]
    if getattr(args,"ws_mode","send")=="echo": cmd += ["--ws-mode","echo"]
    for h in getattr(args,"header",[]) or []: cmd += ["--header",str(h)]
    _info(f"[rust-native] v37 protocol={rust_proto}; runtime_threads={runtime_threads}; workers={rust_workers}; target=1,000,000 RPS on single-core benchmark (target, not guarantee)")
    try:
        proc=subprocess.run(cmd,capture_output=True,text=True,timeout=max(5.0,float(args.duration)+float(args.timeout)+45.0),env={**os.environ,"RUST_BACKTRACE":"1","CARGO_TERM_COLOR":"never"})
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"native Rust watchdog timeout after {exc.timeout:.1f}s") from exc
    if proc.returncode!=0: raise RuntimeError(f"native Rust exit={proc.returncode}: {(proc.stderr or '')[-2400:]}")
    rep=None
    for line in reversed((proc.stdout or '').splitlines()):
        line=line.strip()
        if line.startswith("{") and line.endswith("}"):
            try: rep=json.loads(line);break
            except Exception: pass
    if rep is None: raise RuntimeError("native Rust produced no JSON summary")
    global _RUST_REPORT
    _RUST_REPORT=rep
    return Stats()

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

_TIMEOUT_CTX = getattr(asyncio, "timeout", None)

# ===========================================================================
# [F12] 结构化日志
# ===========================================================================
LOG_FORMAT = "text"
LOG_START = time.time()

def _log(level: str, msg: str):
    if LOG_FORMAT == "json":
        print(json.dumps({
            "ts": round(time.time() - LOG_START, 3),
            "level": level,
            "pid": os.getpid(),
            "msg": msg,
        }, ensure_ascii=False), flush=True)
    else:
        print(f"[{time.time() - LOG_START:7.2f}] [{level}] {msg}", flush=True)

def _info(msg): _log("INFO", msg)
def _warn(msg): _log("WARN", msg)
def _error(msg): _log("ERROR", msg)

# ===========================================================================
# 常量
# ===========================================================================
VERSION = "37.0"
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
H2_MAX_FRAME = 1 << 20
MAX_ERROR_KEYS = 256
DRY_MAX_DRAIN = 128 * 1024 * 1024

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

_CONN_ERROR_NAMES = ("ProtocolError", "ConnectionTerminated", "GoawayError",
                     "HandshakeError", "QuicConnectionError", "StreamClosedError")

def is_conn_level_error(exc: BaseException) -> bool:
    """B9: 优先用类继承判断, 类名匹配仅兜底第三方库异常"""
    if isinstance(exc, asyncio.TimeoutError): return False
    if isinstance(exc, (ConnectionError, ssl.SSLError, OSError)): return True
    try:
        if isinstance(exc, h2.exceptions.H2Error): return True
    except NameError:
        pass
    return type(exc).__name__ in _CONN_ERROR_NAMES

async def supervised(name: str, factory, stop_event: asyncio.Event, stats):
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
            _warn(f"worker {name} crashed: {type(e).__name__}: {e}; restarting in {backoff:.0f}s")
            try:
                if stats is not None:
                    stats.note_swallow(f"worker_crash:{type(e).__name__}")
            except Exception:
                pass
            try: await asyncio.sleep(backoff)
            except asyncio.CancelledError: raise
            backoff = min(backoff * 2.0, 30.0)

def render_template(template: str, data: dict) -> str:
    if not template or data is None: return template
    for k, v in data.items():
        template = template.replace(f"${{{k}}}", str(v))
    return template

async def _run_timed(coro, timeout: float):
    if timeout <= 0:
        raise ValueError("timeout must be > 0")
    if _TIMEOUT_CTX is not None:
        async with _TIMEOUT_CTX(timeout):
            return await coro
    return await asyncio.wait_for(coro, timeout=timeout)

async def open_configured_connection(ep, timeout: float, ssl_ctx=None, socket_config=None):
    family = socket.AF_INET6 if ep.is_v6 else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    if socket_config is not None:
        socket_config(sock)
    sock.setblocking(False)
    address = (ep.ip, ep.port, 0, 0) if ep.is_v6 else (ep.ip, ep.port)
    started = time.monotonic()
    try:
        loop = asyncio.get_running_loop()
        await _run_timed(loop.sock_connect(sock, address), timeout)
        remaining = timeout - (time.monotonic() - started)
        if remaining <= 0:
            raise asyncio.TimeoutError("TCP connection timed out")
        return await _run_timed(
            asyncio.open_connection(
                sock=sock,
                ssl=ssl_ctx,
                server_hostname=ep.hostname if ssl_ctx is not None else None,
            ),
            remaining,
        )
    except BaseException:
        try:
            sock.close()
        except OSError:
            pass
        raise

# ===========================================================================
# [F9] 端点熔断器: CLOSED -> OPEN -> HALF_OPEN
# ===========================================================================
class EndpointHealth:
    def __init__(self, cooldown: float = 2.0, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0):
        self._cooldown = cooldown
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._failed_until: Dict[str, float] = {}
        self._fail_count: Dict[str, int] = {}
        self._open_until: Dict[str, float] = {}
        self._half_open: Dict[str, bool] = {}
        self._probing: Dict[str, bool] = {}
        self._probe_start: Dict[str, float] = {}

    def ready(self, key: str) -> bool:
        now = time.monotonic()
        open_until = self._open_until.get(key)
        # OPEN: 恢复窗口未到, 拒绝
        if open_until is not None and now < open_until:
            return False
        if self._half_open.get(key):
            # HALF_OPEN 已成立: 单飞探测; 探测挂起超过恢复窗口视为丢失, 放行下一次
            if self._probing.get(key):
                if now - self._probe_start.get(key, now) < self._recovery_timeout:
                    return False
                self._probing.pop(key, None)
                self._probe_start.pop(key, None)
            self._probing[key] = True
            self._probe_start[key] = now
            return True
        if open_until is not None:
            # OPEN 窗口到期: 进入 HALF_OPEN, 放行本次探测
            self._half_open[key] = True
            self._probing[key] = True
            self._probe_start[key] = now
            return True
        return now >= self._failed_until.get(key, 0.0)

    def mark_fail(self, key: str):
        now = time.monotonic()
        self._probing.pop(key, None)
        self._probe_start.pop(key, None)
        self._half_open.pop(key, None)
        n = self._fail_count.get(key, 0) + 1
        self._fail_count[key] = n
        if self._failure_threshold > 0 and n >= self._failure_threshold:
            self._open_until[key] = now + self._recovery_timeout
            _warn(f"endpoint {key} circuit OPEN for {self._recovery_timeout:.0f}s "
                  f"after {n} consecutive failures")
        else:
            self._failed_until[key] = now + self._cooldown

    def mark_ok(self, key: str):
        self._probing.pop(key, None)
        self._probe_start.pop(key, None)
        self._half_open.pop(key, None)
        self._failed_until.pop(key, None)
        self._fail_count[key] = 0
        self._open_until.pop(key, None)

    def snapshot(self) -> dict:
        return {
            "cooldowns": len(self._failed_until),
            "open": sum(1 for v in self._open_until.values() if v is not None),
            "half_open": len(self._half_open),
        }

# ===========================================================================
# [F3] 调度器: 背压可见 + drop|block|error 策略
# ===========================================================================
class Scheduler:
    def __init__(self, rate: float, mode: str = "arrival", stop_event: asyncio.Event = None,
                 burst: float = None, queue_size: int = 1024, drop_policy: str = "drop"):
        self.rate = rate
        self.mode = mode
        self.stop_event = stop_event or asyncio.Event()
        self.drop_policy = drop_policy
        self._cond = asyncio.Condition()
        self._queue = asyncio.Queue(maxsize=max(1, queue_size))
        self._task = None
        self.capacity = int(burst if burst is not None else rate) if rate > 0 else 1
        if self.capacity < 1: self.capacity = 1
        self._tokens = float(self.capacity if burst is not None else 0.0)
        self._last_refill = time.monotonic()
        self.dropped_tokens = 0
        self.blocked_ms = 0.0

        if self.mode == "arrival" and self.rate > 0:
            self._task = asyncio.create_task(self._arrival_loop())

    async def _arrival_loop(self):
        interval = 1.0 / self.rate
        next_time = time.monotonic()
        warn_every = 1000
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
                self.dropped_tokens += 1
                if self.drop_policy == "error":
                    self.stop_event.set()
                    _error("arrival queue full and drop_policy=error; stopping")
                    return
                elif self.drop_policy == "block":
                    t0 = time.monotonic()
                    await self._queue.put(True)
                    self.blocked_ms += (time.monotonic() - t0) * 1000.0
                elif self.dropped_tokens % warn_every == 0:
                    _warn(f"Scheduler queue full, dropped {self.dropped_tokens} tokens")

    async def acquire(self) -> bool:
        if self.mode == "arrival" and self.rate > 0:
            while not self.stop_event.is_set():
                try:
                    token = await asyncio.wait_for(self._queue.get(), timeout=0.25)
                except asyncio.TimeoutError:
                    continue
                return token is not None
            return False
        async with self._cond:
            while not self.stop_event.is_set():
                if self.rate <= 0:
                    return True
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._last_refill = now
                self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
                wait_time = (1.0 - self._tokens) / max(self.rate, 0.001)
                try:
                    await asyncio.wait_for(self._cond.wait(), timeout=wait_time)
                except asyncio.TimeoutError:
                    pass
        return False

    def stop(self):
        self.stop_event.set()
        if self._task:
            self._task.cancel()
        try:
            self._queue.put_nowait(None)
        except asyncio.QueueFull:
            pass

    def metrics(self) -> dict:
        return {"dropped_tokens": self.dropped_tokens,
                "blocked_ms": round(self.blocked_ms, 1)}


class OneShotScheduler:
    """dry-run: 每个 worker 最多一个令牌"""
    dropped_tokens = 0
    def __init__(self, stop_event: asyncio.Event):
        self._issued = False
        self._stop_event = stop_event
        self.blocked_ms = 0.0

    async def acquire(self) -> bool:
        if self._issued or self._stop_event.is_set():
            return False
        self._issued = True
        return True

    def stop(self):
        self._stop_event.set()

    def metrics(self) -> dict:
        return {"dropped_tokens": 0, "blocked_ms": 0.0}
# ===========================================================================
# [F5] 精确 HDR 直方图: 十进制 100 子桶, 1us..1000s, 分位线性插值
# ===========================================================================
class PreciseHistogram:
    """
    单位: 微秒。范围 [1us, 10^9 us); >=10^9 进溢出桶 999。
    order = floor(log10(us)); 每 order 100 子桶, 完整线性覆盖
    [10^order, 10^(order+1)) 整个数量级, 桶宽 = 9*10^order/100。
    encode 与 decode 互为逆映射; 分位数在子桶内线性插值,
    桶内相对误差 <= 9% (数量级下界) 且随数值增大降至 <1%。
    """
    __slots__ = ("buckets", "count", "min_val", "max_val")
    SUB_BUCKETS = 100
    NB = 1000
    OVERFLOW_US = 10 ** 9

    def __init__(self):
        self.buckets = [0] * self.NB
        self.count = 0
        self.min_val = float('inf')
        self.max_val = 0.0

    @classmethod
    def _encode(cls, us: int) -> int:
        if us <= 0:
            return 0
        if us >= cls.OVERFLOW_US:
            return cls.NB - 1
        order = 0
        nxt = 10
        while nxt <= us:
            order += 1
            nxt *= 10
        if order == 0:
            return us - 1  # 1..10us -> idx 0..9 (单 us 桶)
        pow10 = 10 ** order
        # 完整覆盖 [pow10, 10*pow10): 桶宽 = 9*pow10/100
        sub = (us - pow10) * cls.SUB_BUCKETS // (pow10 * 9)
        if sub >= cls.SUB_BUCKETS:
            sub = cls.SUB_BUCKETS - 1
        return order * cls.SUB_BUCKETS + sub

    @staticmethod
    def _decode(idx: int) -> float:
        """返回该桶下界(us)。与 _encode 互为逆: 对任意 idx, _encode(下界)<=idx<_encode(上界)。

        注意: 本函数在类体预计算 _LOWER/_UPPER 时即被调用, 此时类尚未创建完成,
        不能引用 cls, 故常量以字面量形式内联 (SUB_BUCKETS=100)。"""
        if idx <= 0:
            return 1.0
        if idx >= 999:
            return float(10 ** 9)
        order = idx // 100
        sub = idx % 100
        if order == 0:
            return float(1 + idx)  # 1..10us 单 us 桶
        pow10 = 10 ** order
        # 桶宽 = 9*pow10/100, 浮点边界避免整数除法精度丢失
        return pow10 + sub * (pow10 * 9) / 100.0

    # 预计算桶边界, 分位热路径零除法
    _LOWER = [0] * NB
    _UPPER = [0] * NB
    for _i in range(NB):
        _LOWER[_i] = _decode(_i)
        _UPPER[_i] = _decode(_i + 1) if _i < NB - 1 else OVERFLOW_US

    def add(self, ms: float):
        self.count += 1
        if ms < self.min_val: self.min_val = ms
        if ms > self.max_val: self.max_val = ms
        self.buckets[self._encode(int(ms * 1000))] += 1

    def percentile(self, p: float) -> float:
        if self.count == 0:
            return 0.0
        target = self.count * (p / 100.0)
        c = 0
        buckets = self.buckets
        lower = self._LOWER
        upper = self._UPPER
        for i in range(self.NB):
            b = buckets[i]
            if c + b >= target:
                frac = (target - c) / b if b else 0.5
                return (lower[i] + (upper[i] - lower[i]) * frac) / 1000.0
            c += b
        return self.max_val


def merged_percentile(hist: list, count: int, p: float) -> float:
    if count == 0:
        return 0.0
    target = count * (p / 100.0)
    c = 0
    lower = PreciseHistogram._LOWER
    upper = PreciseHistogram._UPPER
    for i in range(len(hist)):
        b = hist[i]
        if c + b >= target:
            frac = (target - c) / b if b else 0.5
            return (lower[i] + (upper[i] - lower[i]) * frac) / 1000.0
        c += b
    return 0.0


# ===========================================================================
# v32 Unified Scenario IR + client-side runtime telemetry
# ===========================================================================
@dataclass(frozen=True)
class ScenarioIR:
    """Canonical, hashable description of one load-test scenario.

    The IR is deliberately protocol-neutral. Engines consume it through the
    existing args/group adapters; the digest provides reproducibility and makes
    engine/config drift visible in reports.
    """
    targets: Tuple[str, ...]
    method: str
    path: str
    body_sha256: str
    headers_sha256: str
    workers: int
    processes: int
    duration: float
    warmup: float
    timeout: float
    mode: str
    engine: str
    rps: float
    rate_mode: str
    rps_burst: float
    arrival_queue: int
    arrival_drop_policy: str
    keepalive: bool
    skip_body: bool
    ignore_status: bool
    randomize: bool
    max_conns_per_ep: int
    h2_conns_per_ep: int
    benchmark: str = "off"
    security_layer: str = "all"
    rust_protocol: str = "auto"

    def canonical_json(self) -> str:
        return json.dumps({
            "targets": list(self.targets), "method": self.method, "path": self.path,
            "body_sha256": self.body_sha256, "headers_sha256": self.headers_sha256,
            "workers": self.workers, "processes": self.processes,
            "duration": self.duration, "warmup": self.warmup, "timeout": self.timeout,
            "mode": self.mode, "engine": self.engine, "rps": self.rps,
            "rate_mode": self.rate_mode, "rps_burst": self.rps_burst,
            "arrival_queue": self.arrival_queue, "arrival_drop_policy": self.arrival_drop_policy,
            "keepalive": self.keepalive, "skip_body": self.skip_body,
            "ignore_status": self.ignore_status, "randomize": self.randomize,
            "max_conns_per_ep": self.max_conns_per_ep, "h2_conns_per_ep": self.h2_conns_per_ep,
            "benchmark": self.benchmark, "security_layer": self.security_layer, "rust_protocol": self.rust_protocol,
        }, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()[:16]


def _headers_fingerprint(header_list: List[str]) -> str:
    normalized = [str(x).strip() for x in header_list]
    return hashlib.sha256("\n".join(sorted(normalized)).encode("utf-8")).hexdigest()


def build_scenario_ir(args, norm_targets, body: bytes, resolved_engine: str) -> ScenarioIR:
    control_material = list(getattr(args, "header", []) or [])
    control_material.extend([
        f"__security_layer__={getattr(args, 'security_layer', 'all')}",
        f"__benchmark__={getattr(args, 'benchmark', 'off')}",
        f"__rust_protocol__={getattr(args, 'rust_protocol', 'auto')}",
        f"__body_mode__={'discard' if getattr(args, 'skip_body', False) else 'count'}",
        f"__rust_runtime_threads__={getattr(args, 'rust_runtime_threads', 0)}",
        f"__cpu_core__={getattr(args, 'cpu_core', None)}",
        f"__h3_conns__={getattr(args, 'h3_conns', 1)}",
        f"__ws_mode__={getattr(args, 'ws_mode', 'send')}",
    ])
    return ScenarioIR(
        targets=tuple(norm_targets), method=str(args.method).upper(), path=str(args.path),
        body_sha256=hashlib.sha256(body or b"").hexdigest(),
        headers_sha256=_headers_fingerprint(control_material),
        workers=int(args.workers), processes=int(getattr(args, "processes", 1)),
        duration=float(args.duration), warmup=float(args.warmup), timeout=float(args.timeout),
        mode=str(args.mode), engine=str(resolved_engine), rps=float(args.rps),
        rate_mode=str(args.rate_mode), rps_burst=float(args.rps_burst or 0.0),
        arrival_queue=int(args.arrival_queue), arrival_drop_policy=str(args.arrival_drop_policy),
        keepalive=not bool(getattr(args, "no_keepalive", False)),
        skip_body=bool(args.skip_body), ignore_status=bool(args.ignore_status),
        randomize=bool(args.randomize), max_conns_per_ep=int(args.max_conns_per_ep),
        h2_conns_per_ep=int(args.h2_conns_per_ep), benchmark=str(getattr(args, "benchmark", "off")),
        security_layer=str(getattr(args, "security_layer", "all")), rust_protocol=str(getattr(args, "rust_protocol", "auto")),
    )


class RuntimeTelemetry:
    """Low-overhead process telemetry for distinguishing client saturation from server limits."""
    __slots__ = ("interval", "samples", "max_cpu_pct", "max_rss_mb", "max_fd",
                 "max_scheduler_lag_ms", "_last_cpu", "_last_wall", "_loop_lag_max", "max_cpu_one_core_pct")

    def __init__(self, interval: float = 1.0):
        self.interval = max(0.2, float(interval))
        self.samples = 0
        self.max_cpu_pct = 0.0
        self.max_rss_mb = 0.0
        self.max_fd = 0
        self.max_scheduler_lag_ms = 0.0
        self._last_cpu = time.process_time()
        self._last_wall = time.monotonic()
        self._loop_lag_max = 0.0
        self.max_cpu_one_core_pct = 0.0

    def _rss_mb(self) -> float:
        try:
            # Linux ru_maxrss is KiB; macOS is bytes.
            raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return raw / 1024.0 if sys.platform.startswith("linux") else raw / (1024.0 * 1024.0)
        except Exception:
            return 0.0

    def _fd_count(self) -> int:
        try:
            return len(os.listdir("/proc/self/fd"))
        except Exception:
            return 0

    async def run(self, stop_event: asyncio.Event):
        next_tick = time.monotonic() + self.interval
        while not stop_event.is_set():
            now = time.monotonic()
            lag = max(0.0, (now - next_tick) * 1000.0)
            self.max_scheduler_lag_ms = max(self.max_scheduler_lag_ms, lag)
            if now < next_tick:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=next_tick - now)
                except asyncio.TimeoutError:
                    pass
                continue
            wall = now - self._last_wall
            cpu = time.process_time() - self._last_cpu
            if wall > 0:
                cores = max(1, os.cpu_count() or 1)
                self.max_cpu_pct = max(self.max_cpu_pct, min(100.0, cpu / wall * 100.0))
                self.max_cpu_one_core_pct = max(self.max_cpu_one_core_pct, min(100.0, cpu / wall * 100.0 / cores))
            self.max_rss_mb = max(self.max_rss_mb, self._rss_mb())
            self.max_fd = max(self.max_fd, self._fd_count())
            self.samples += 1
            self._last_wall = now
            self._last_cpu = time.process_time()
            next_tick = now + self.interval

    def classification(self, stats, fd_limit: int) -> str:
        # Conservative: mark client-limited only when multiple client-side signals agree.
        signals = 0
        if self.max_cpu_one_core_pct >= 90.0: signals += 1
        if fd_limit > 0 and self.max_fd >= int(fd_limit * 0.90): signals += 1
        if self.max_scheduler_lag_ms >= max(20.0, self.interval * 1000.0 * 0.20): signals += 1
        if signals >= 2:
            return "CLIENT_LIMITED"
        if stats.dropped_tokens > 0 or self.max_scheduler_lag_ms >= max(50.0, self.interval * 1000.0 * 0.50):
            return "SCHEDULER_LIMITED"
        return "UNKNOWN"

# ===========================================================================
# [B5/B6/B7/B8] 统计: TTFB + in-flight + 截断计数 + 采样导出 + swallow 计数
# ===========================================================================
class Stats:
    __slots__ = ("requests", "success", "failed", "bytes_in", "bytes_out",
                 "status_codes", "errors", "swallowed",
                 "lat_sum", "lat_min", "lat_max",
                 "ttfb_sum", "ttfb_min", "ttfb_max",
                 "_hist", "_ttfb_hist", "_per_endpoint",
                 "_detail_enabled", "_detail_file", "_csv_writer",
                 "_detail_sample", "_detail_count",
                 "_consecutive_fails", "start_time", "start_mono",
                 "_timeline_rps", "_timeline_lat", "h2_pushes",
                 "_recording", "connect_ms_sum", "connect_count",
                 "warmup_requests", "truncated_responses", "in_flight",
                 "peak_in_flight", "dropped_tokens", "blocked_ms", "scenario_digest", "client_classification", "runtime_cpu_max", "runtime_rss_max_mb", "runtime_fd_max", "runtime_scheduler_lag_max_ms")

    def __init__(self):
        self.requests = 0
        self.success = 0
        self.failed = 0
        self.bytes_in = 0
        self.bytes_out = 0
        self.status_codes = {}
        self.errors = defaultdict(int)
        self.swallowed = defaultdict(int)
        self.lat_sum = 0.0
        self.lat_min = None
        self.lat_max = 0.0
        self.ttfb_sum = 0.0
        self.ttfb_min = None
        self.ttfb_max = 0.0
        self._hist = PreciseHistogram()
        self._ttfb_hist = PreciseHistogram()
        self._per_endpoint = defaultdict(int)
        self._detail_enabled = False
        self._detail_file = None
        self._csv_writer = None
        self._detail_sample = 1
        self._detail_count = 0
        self._consecutive_fails = 0
        self.start_time = time.time()
        self.start_mono = time.monotonic()
        self._timeline_rps = deque(maxlen=3600)
        self._timeline_lat = deque(maxlen=3600)
        self.h2_pushes = 0
        self._recording = True
        self.connect_ms_sum = 0.0
        self.connect_count = 0
        self.warmup_requests = 0
        self.truncated_responses = 0
        self.in_flight = 0
        self.peak_in_flight = 0
        self.dropped_tokens = 0
        self.blocked_ms = 0.0
        self.scenario_digest = ""
        self.client_classification = "UNKNOWN"
        self.runtime_cpu_max = 0.0
        self.runtime_rss_max_mb = 0.0
        self.runtime_fd_max = 0
        self.runtime_scheduler_lag_max_ms = 0.0

    def note_swallow(self, tag: str):
        if tag in self.swallowed:
            self.swallowed[tag] += 1
        elif len(self.swallowed) < MAX_ERROR_KEYS:
            self.swallowed[tag] = 1

    def begin_request(self):
        self.in_flight += 1
        if self.in_flight > self.peak_in_flight:
            self.peak_in_flight = self.in_flight

    def end_request(self):
        if self.in_flight > 0:
            self.in_flight -= 1

    def enable_detail_log(self, filename: str, fmt: str = 'csv', sample: int = 1):
        self._detail_enabled = True
        self._detail_sample = max(1, sample)
        if fmt == 'csv':
            self._detail_file = open(filename, 'w', encoding='utf-8', newline='')
            self._csv_writer = csv.writer(self._detail_file)
            self._csv_writer.writerow(['timestamp', 'status', 'latency_ms', 'ttfb_ms',
                                       'success', 'error', 'endpoint'])
        elif fmt == 'json':
            self._detail_file = open(filename, 'wb')

    def record(self, status=0, bytes_in=0, bytes_out=0, ms=0.0, ok=True,
               err=None, endpoint="", circuit_breaker=0, stop_event=None,
               h2_push=0, ttfb=None):
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
                _error(f"Circuit breaker tripped: {self._consecutive_fails} consecutive fails.")
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
        if ttfb is not None and ttfb >= 0:
            self.ttfb_sum += ttfb
            if self.ttfb_min is None or ttfb < self.ttfb_min: self.ttfb_min = ttfb
            if ttfb > self.ttfb_max: self.ttfb_max = ttfb
            self._ttfb_hist.add(ttfb)
        if self._detail_enabled and (self._detail_sample == 1 or self.requests % self._detail_sample == 0):
            self._detail_count += 1
            if self._csv_writer:
                self._csv_writer.writerow([time.time(), status, round(ms, 3),
                                           round(ttfb, 3) if ttfb is not None else -1,
                                           ok, err or '', endpoint])
            elif self._detail_file:
                line = json.dumps({'ts': time.time(), 'st': status, 'ms': round(ms, 3),
                                   'ttfb': round(ttfb, 3) if ttfb is not None else -1,
                                   'ok': ok, 'err': err, 'ep': endpoint})
                self._detail_file.write((line + '\n').encode())
            if self._detail_count % 10000 == 0 and self._detail_file is not None:
                try: self._detail_file.flush()
                except Exception: pass

    def record_connect(self, ms: float):
        if not self._recording: return
        self.connect_ms_sum += ms
        self.connect_count += 1

    def reset(self):
        self.requests = 0; self.success = 0; self.failed = 0
        self.bytes_in = 0; self.bytes_out = 0
        self.status_codes = {}; self.errors = defaultdict(int)
        self.lat_sum = 0.0; self.lat_min = None; self.lat_max = 0.0
        self.ttfb_sum = 0.0; self.ttfb_min = None; self.ttfb_max = 0.0
        self._hist = PreciseHistogram(); self._ttfb_hist = PreciseHistogram()
        self._per_endpoint = defaultdict(int)
        self._consecutive_fails = 0
        self.start_time = time.time(); self.start_mono = time.monotonic()
        self.h2_pushes = 0
        self.connect_ms_sum = 0.0; self.connect_count = 0
        self.warmup_requests = 0; self.truncated_responses = 0
        self._timeline_rps.clear(); self._timeline_lat.clear()
        self.peak_in_flight = self.in_flight  # V4: 保留预热结束仍在飞的并发作为基线
        self._recording = True

    def record_timeline(self, elapsed: float, rps: float, lat_p50: float, lat_p99: float):
        self._timeline_rps.append((elapsed, rps))
        self._timeline_lat.append((elapsed, lat_p50, lat_p99))

    def snapshot(self, wid=0, final=False) -> dict:
        return {
            "wid": wid, "final": final,
            "req": self.requests, "ok": self.success, "fail": self.failed,
            "bytes_in": self.bytes_in, "bytes_out": self.bytes_out,
            "lat_sum": self.lat_sum,
            "lat_min": self.lat_min if self.lat_min is not None else -1.0,
            "lat_max": self.lat_max,
            "ttfb_sum": self.ttfb_sum,
            "ttfb_min": self.ttfb_min if self.ttfb_min is not None else -1.0,
            "ttfb_max": self.ttfb_max,
            "h2_pushes": self.h2_pushes,
            "count": self._hist.count,
            "hist": list(self._hist.buckets),
            "ttfb_hist": list(self._ttfb_hist.buckets),
            "status_codes": dict(self.status_codes),
            "connect_ms_sum": self.connect_ms_sum,
            "connect_count": self.connect_count,
            "truncated": self.truncated_responses,
            "peak_in_flight": self.peak_in_flight,
            "scenario_digest": self.scenario_digest,
            "client_classification": self.client_classification,
            "runtime_cpu_max": self.runtime_cpu_max,
            "runtime_rss_max_mb": self.runtime_rss_max_mb,
            "runtime_fd_max": self.runtime_fd_max,
            "runtime_scheduler_lag_max_ms": self.runtime_scheduler_lag_max_ms,
        }

    @property
    def p50(self): return self._hist.percentile(50)
    @property
    def p95(self): return self._hist.percentile(95)
    @property
    def p99(self): return self._hist.percentile(99)
    @property
    def p999(self): return self._hist.percentile(99.9)
    @property
    def ttfb_p50(self): return self._ttfb_hist.percentile(50)
    @property
    def ttfb_p99(self): return self._ttfb_hist.percentile(99)
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
# 集群聚合器 (含 TTFB 合并 + 失联检测)
# ===========================================================================
class ClusterAggregator:
    def __init__(self):
        self.latest: Dict[Any, dict] = {}
        self.registered: set = set()
        self.start_time = time.monotonic()
        self.missing_final: set = set()
        self.timed_out: set = set()  # [FIX3] 心跳超时被剔除的 worker

    def drop_worker(self, wid):
        # [FIX3] 心跳超时: 同步剔除 latest 中非 final 快照, 避免 all_done 永假挂死
        self.latest.pop(wid, None)
        self.registered.discard(wid)
        self.timed_out.add(wid)

    def register(self, wid):
        self.registered.add(wid)

    def apply(self, snap: dict):
        wid = snap.get("wid", 0)
        self.latest[wid] = snap

    @property
    def all_done(self):
        return bool(self.latest) and all(s.get("final") for s in self.latest.values())

    def missing(self):
        """已注册但未上报 final 的 worker (失联/快照丢失) — B: 数据完整性告警"""
        return {w for w in self.registered if not self.latest.get(w, {}).get("final")}

    def totals(self) -> dict:
        req = ok = fail = bi = bo = pushes = count = trunc = 0
        cms = 0.0; cc = 0
        lat_sum = 0.0; lat_min = None; lat_max = 0.0
        ttfb_sum = 0.0; ttfb_min = None; ttfb_max = 0.0
        hist = [0] * 1000
        ttfb_hist = [0] * 1000
        status_codes = defaultdict(int)
        peak_if = 0
        for s in self.latest.values():
            req += s["req"]; ok += s["ok"]; fail += s["fail"]
            bi += s["bytes_in"]; bo += s["bytes_out"]
            pushes += s["h2_pushes"]; count += s["count"]
            trunc += s.get("truncated", 0)
            lat_sum += s["lat_sum"]
            lm = s.get("lat_min", -1.0)
            if lm is not None and lm >= 0:
                lat_min = lm if lat_min is None else min(lat_min, lm)
            if s["lat_max"] > lat_max: lat_max = s["lat_max"]
            ttfb_sum += s.get("ttfb_sum", 0.0)
            tm = s.get("ttfb_min", -1.0)
            if tm is not None and tm >= 0:
                ttfb_min = tm if ttfb_min is None else min(ttfb_min, tm)
            if s.get("ttfb_max", 0) > ttfb_max: ttfb_max = s["ttfb_max"]
            cms += s.get("connect_ms_sum", 0.0)
            cc += s.get("connect_count", 0)
            peak_if = max(peak_if, s.get("peak_in_flight", 0))
            h = s.get("hist") or []
            for i in range(min(1000, len(h))): hist[i] += h[i]
            th = s.get("ttfb_hist") or []
            for i in range(min(1000, len(th))): ttfb_hist[i] += th[i]
            for k, v in (s.get("status_codes") or {}).items():
                status_codes[k] += v
        return {"req": req, "ok": ok, "fail": fail, "bytes_in": bi, "bytes_out": bo,
                "lat_sum": lat_sum, "lat_min": lat_min, "lat_max": lat_max,
                "ttfb_sum": ttfb_sum, "ttfb_min": ttfb_min, "ttfb_max": ttfb_max,
                "h2_pushes": pushes, "count": count, "hist": hist,
                "ttfb_hist": ttfb_hist, "status_codes": dict(status_codes),
                "connect_ms_sum": cms, "connect_count": cc,
                "truncated": trunc, "peak_in_flight": peak_if}


class MergedStatsView:
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
    def p999(self): return merged_percentile(self._t["hist"], self._t["count"], 99.9)
    @property
    def error_rate(self):
        return self.failed / self.requests if self.requests > 0 else 0.0


def load_yaml_config(filepath: str) -> dict:
    if not HAS_YAML:
        _error("YAML 配置需要: pip install pyyaml"); sys.exit(1)
    try:
        with open(filepath, 'r', encoding='utf-8') as f: return yaml.safe_load(f)
    except Exception as e:
        _error(f"解析 YAML 失败: {e}"); sys.exit(1)


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
            loop = asyncio.get_running_loop()
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
    def _probe_blocking(ip, port, hostname, scheme, insecure, timeout) -> Endpoint:
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
    async def probe_endpoint(ip, port, hostname, scheme, insecure, timeout=5.0) -> Endpoint:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, ServerDiscovery._probe_blocking, ip, port, hostname, scheme, insecure, timeout)

    @staticmethod
    async def discover(target_url, default_path, method, body, insecure, timeout=5.0, prefer_ips=None) -> TargetGroup:
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
        group = TargetGroup(hostname=hostname, port=port, scheme=scheme, base_path=base_path,
                            base_query=base_query, method=method, body=body, host_hdr=host_hdr)
        _info(f"\n[DISCOVERY] {scheme}://{hostname}:{port}\n  Path: {base_path}")
        ip_list = await ServerDiscovery.resolve_dns(hostname, use_aiodns=HAS_AIODNS)
        if prefer_ips: ip_list = [(ip, ':' in ip) for ip in prefer_ips if ip]
        if not ip_list: ip_list = [(hostname, ':' in hostname)]
        _info(f"  DNS resolved: {len(ip_list)} IP(s)")
        for ip, is_v6 in ip_list: _info(f"    {ip}{' (IPv6)' if is_v6 else ''}")
        tasks = [ServerDiscovery.probe_endpoint(ip, port, hostname, scheme, insecure, timeout) for ip, _ in ip_list]
        endpoints = await asyncio.gather(*tasks, return_exceptions=True)
        for ep in endpoints:
            if isinstance(ep, Exception):
                _warn(f"probe failed: {ep}"); continue
            if ep:
                group.endpoints.append(ep)
                h3_info = f", h3_port={ep.h3_port}" if ep.h3_port else ""
                _info(f"  Endpoint: {ep.display}  server={ep.server_hdr or '?'}{h3_info}")
        if not group.endpoints:
            ep = Endpoint(ip=hostname, port=port, hostname=hostname, scheme=scheme,
                          is_v6=(':' in hostname), alpn="http/1.1")
            group.endpoints.append(ep)
            _info(f"  [FALLBACK] using hostname directly: {ep.display}")
        _info(f"  Total endpoints: {len(group.endpoints)}")
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


# ===========================================================================
# [B3] HTTP/1.1 响应解析: 截断时抛异常而非静默
# ===========================================================================
class H1HeaderTruncated(Exception):
    pass

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
                raise H1HeaderTruncated("response headers exceed 1MB")
    header_data = bytes(buf[:hdr_end + 4])
    leftover = bytes(buf[hdr_end + 4:])
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


async def drain_h1_body(reader, chunked, clen, leftover=b"", max_drain=DRY_MAX_DRAIN):
    """返回 (drained_bytes, truncated)"""
    if chunked:
        buf = bytearray(leftover); drained = 0; scan = 0
        while drained < max_drain:
            while True:
                idx = buf.find(b"\r\n", scan)
                if idx != -1: break
                scan = max(0, len(buf) - 1)
                chunk = await reader.read(65536)
                if not chunk: return drained, False
                buf.extend(chunk)
                if len(buf) > MAX_H1_CHUNK_LINE: return drained, False
            size_line = bytes(buf[:idx]); del buf[:idx + 2]; scan = 0
            try: size = int(size_line.strip().split(b";", 1)[0], 16)
            except ValueError: return drained, False
            if size == 0:
                while True:
                    idx = buf.find(b"\r\n")
                    if idx == -1:
                        chunk = await reader.read(8192)
                        if not chunk: return drained, False
                        buf.extend(chunk)
                        continue
                    line = bytes(buf[:idx]); del buf[:idx + 2]
                    if not line: break
                return drained, False
            remaining = size + 2
            while remaining > 0:
                if buf:
                    take = min(len(buf), remaining); del buf[:take]; remaining -= take
                else:
                    chunk = await reader.read(min(remaining, 65536))
                    if not chunk: return drained, False
                    remaining -= len(chunk)
            drained += size
        return drained, True
    if clen is not None:
        remaining = min(clen, max_drain)
        if leftover:
            take = min(len(leftover), remaining); remaining -= take
        while remaining > 0:
            chunk = await reader.read(min(remaining, 65536))
            if not chunk: break
            remaining -= len(chunk)
        return min(clen, max_drain), clen > max_drain
    drained = len(leftover)
    while drained < max_drain:
        chunk = await reader.read(65536)
        if not chunk: break
        drained += len(chunk)
    return drained, drained >= max_drain


# ===========================================================================
# 连接池
# ===========================================================================
class ConnPool:
    def __init__(self, max_per_ep: int = 64):
        self._pools: Dict[str, asyncio.LifoQueue] = {}
        self._max = max_per_ep

    def get(self, key):
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
                hdrs = dict(base_extra)
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
# Raw HTTP/1.1 引擎 (B6: TTFB; B7: in-flight; B5: bytes_in 语义)
# ===========================================================================
async def _raw_request_session(reader, writer, req_bytes, group, args, ep, small_segs=False):
    """返回 (status, body_bytes_received, close_flag, ttfb_ms)"""
    t0 = time.perf_counter()
    if small_segs:
        pos = 0; total = len(req_bytes)
        while pos < total:
            take = random.randint(1, 16)
            writer.write(req_bytes[pos:pos + take])
            await writer.drain()
            pos += take
    else:
        writer.write(req_bytes)
        await writer.drain()
    # 读 header, TTFB = 收到 header 完成时刻
    status, chunked, clen, close_flag, leftover = await read_h1_response(reader)
    ttfb_ms = (time.perf_counter() - t0) * 1000.0
    if status in (204, 304) or group.method.upper() == "HEAD":
        return status, 0, close_flag, ttfb_ms
    drained, truncated = await drain_h1_body(reader, chunked, clen, leftover)
    return status, drained, close_flag, ttfb_ms


async def raw_worker(groups, args, ssl_ctx, tcp_intf, health, stats, stop_event, scheduler, data_pool=None):
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
        key = f"{ep.ip}:{ep.port}"
        if not health.ready(key): raise ConnectionError(f"endpoint {key} cooling down")
        conn = pool.get(key)
        if conn is not None:
            return conn[0], conn[1], True
        use_ssl = ep.scheme == "https"
        t0 = time.perf_counter()
        try:
            reader, writer = await open_configured_connection(
                ep, args.timeout,
                ssl_ctx=ep_ssl_ctxs.get((ep.ip, ep.hostname)) if use_ssl else None,
                socket_config=tcp_intf.apply,
            )
            health.mark_ok(key)
        except Exception:
            health.mark_fail(key)
            raise
        stats.record_connect((time.perf_counter() - t0) * 1000.0)
        return reader, writer, False

    try:
        while not stop_event.is_set():
            if scheduler and not await scheduler.acquire(): break
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

            retried = False
            while True:
                start = time.perf_counter()
                conn = None
                reused = False
                stats.begin_request()
                try:
                    reader, writer, reused = await get_conn(ep)
                    conn = (reader, writer)
                    status, body_len, close_flag, ttfb = await _run_timed(
                        _raw_request_session(reader, writer, req_bytes, group, args, ep, small_segs),
                        args.timeout)
                    ms = (time.perf_counter() - start) * 1000.0
                    ok = True if args.ignore_status else (status < 400 if status else False)
                    stats.record(status=status, bytes_in=body_len, bytes_out=len(req_bytes),
                                 ms=ms, ok=ok, endpoint=ep.display,
                                 circuit_breaker=args.circuit_breaker, stop_event=stop_event,
                                 ttfb=ttfb)
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
                    # R1: 熔断冷却期的请求被 gate 拦截 -> 归入 swallowed, 不计错误率
                    if isinstance(e, ConnectionError) and "cooling down" in str(e):
                        stats.note_swallow("breaker_open")
                        await asyncio.sleep(0.01)
                        break
                    if (not retried and reused and is_conn_level_error(e)
                            and not stop_event.is_set()):
                        retried = True
                        continue
                    ms = (time.perf_counter() - start) * 1000.0
                    stats.record(status=0, ms=ms, ok=False, err=type(e).__name__,
                                 endpoint=ep.display,
                                 circuit_breaker=args.circuit_breaker, stop_event=stop_event)
                    break
                finally:
                    stats.end_request()

            req_count += 1
            if tcp_intf.churn_interval > 0 and req_count % tcp_intf.churn_interval == 0:
                for g in groups:
                    for e in g.endpoints: pool.evict_all(f"{e.ip}:{e.port}")
    finally:
        for g in groups:
            for e in g.endpoints: pool.evict_all(f"{e.ip}:{e.port}")

# ===========================================================================
# [F4] HTTP/2 连接: 动态流控窗口 + 根因可见 (B2)
# ===========================================================================
class H2Connection:
    def __init__(self, reader, writer, ep, group, timeout: float = 10.0):
        self.reader = reader; self.writer = writer; self.ep = ep; self.group = group
        self._timeout = timeout
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
        self._init_window = 65535  # [F4] 真实协商值, init 后更新

    async def init(self):
        async with self._conn_lock:
            self.conn.initiate_connection()
            # [F4] 请求大窗口, 但以服务端实际 SETTINGS 为准
            try:
                self.conn.update_settings({
                    h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 1 << 20,
                    h2.settings.SettingCodes.MAX_FRAME_SIZE: H2_MAX_FRAME,
                })
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
        # [B2] 接收循环崩溃: 记录根因并标记连接失效, 不再静默
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
                        if isinstance(event, h2.events.ResponseReceived):
                            events_to_dispatch.append((self._streams.get(stream_id), event))
                        elif isinstance(event, h2.events.DataReceived):
                            self.conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                            events_to_dispatch.append((self._streams.get(stream_id), event))
                        elif isinstance(event, h2.events.StreamEnded):
                            events_to_dispatch.append((self._streams.get(stream_id), event))
                        elif isinstance(event, h2.events.StreamReset):
                            events_to_dispatch.append((self._streams.get(stream_id), event))
                        elif isinstance(event, h2.events.RemoteSettingsChanged):
                            new_max = event.changed_settings.get(h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS)
                            if new_max and new_max.new_value: self._max_concurrent = new_max.new_value
                            # [F4] 服务端 INITIAL_WINDOW_SIZE 才是真实可用窗口
                            new_win = event.changed_settings.get(h2.settings.SettingCodes.INITIAL_WINDOW_SIZE)
                            if new_win and new_win.new_value:
                                self._init_window = new_win.new_value
                                for sid, ev in list(self._window_events.items()): ev.set()
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
                        elif isinstance(event, h2.events.ConnectionTerminated):
                            self._closed = True; break
                    data_to_send = self.conn.data_to_send()
                    if data_to_send: self.writer.write(data_to_send)
                for q, event in events_to_dispatch:
                    if q is not None: self._safe_put(q, event)
                if data_to_send:
                    try: await self.writer.drain()
                    except Exception: self._closed = True; break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # [B2] 根因可见
            _error(f"h2 receiver loop crashed on {self.ep.display}: {type(e).__name__}: {e}")
            self._closed = True
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

    async def send_request(self, path, headers, body=None) -> Tuple[int, int, int, int]:
        """返回 (status, resp_bytes, ttfb_ms, pushes); 流控按真实协商窗口取数 [F4]"""
        async with self._stream_cond:
            while self._active_streams >= self._max_concurrent and not self._closed:
                try: await asyncio.wait_for(self._stream_cond.wait(), timeout=1.0)
                except asyncio.TimeoutError: pass
            if self._closed: raise ConnectionError("h2 connection closed")
            self._active_streams += 1

        stream_id = None; completed = False; win_ev = None; pushes = 0
        start_time = time.monotonic()
        ttfb = None
        try:
            async with self._conn_lock:
                if self._closed: raise ConnectionError("h2 connection closed")
                stream_id = self.next_stream_id; self.next_stream_id += 2
                self._streams[stream_id] = asyncio.Queue(maxsize=512)

                req_headers = [(":method", self.group.method), (":path", path),
                               (":scheme", self.ep.scheme), (":authority", self.group.host_hdr)]
                for k, v in headers.items():
                    if k.lower() not in ("host", "connection", "transfer-encoding", "content-length", "keep-alive"):
                        req_headers.append((k, v))
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
                            # [F4] 精确流控: 取窗口/帧/剩余三者最小
                            window = self.conn.local_flow_control_window(stream_id)
                            if window > 0:
                                take = min(window, H2_MAX_FRAME, total - pos)
                                end = (pos + take >= total)
                                self.conn.send_data(stream_id, body_view[pos:pos + take], end_stream=end)
                                pos += take; sent = True
                                data = self.conn.data_to_send()
                                if data: self.writer.write(data)
                        except h2.exceptions.FlowControlError:
                            sent = False
                    try: await self.writer.drain()
                    except Exception: raise ConnectionError("h2 drain failed during body")
                    if not sent:
                        remaining = self._timeout - (time.monotonic() - start_time)
                        if remaining <= 0: raise asyncio.TimeoutError("h2 flow-control window stalled")
                        try: await asyncio.wait_for(win_ev.wait(), timeout=min(1.0, remaining))
                        except asyncio.TimeoutError: pass
                        if self._closed: raise ConnectionError("h2 connection closed during wait")
                        # [v23-B] 窗口事件可能来自其它流, 不清空, 避免吞唤醒
                        win_ev.clear()

            status = 0; resp_len = 0; body_complete = False
            while not body_complete:
                remaining = self._timeout - (time.monotonic() - start_time)
                if remaining <= 0:
                    raise asyncio.TimeoutError("h2 response timed out")
                try:
                    event = await asyncio.wait_for(self._streams[stream_id].get(), timeout=remaining)
                except asyncio.TimeoutError as exc:
                    raise asyncio.TimeoutError("h2 response timed out") from exc
                if event is None:
                    raise ConnectionError("h2 connection closed before response completed")
                if isinstance(event, h2.events.ResponseReceived):
                    if ttfb is None:
                        ttfb = (time.monotonic() - start_time) * 1000.0
                    for k, v in event.headers:
                        if k == ":status":
                            try: status = int(v)
                            except ValueError: pass
                elif isinstance(event, h2.events.DataReceived):
                    if ttfb is None:
                        ttfb = (time.monotonic() - start_time) * 1000.0
                    resp_len += event.flow_controlled_length
                elif isinstance(event, h2.events.PushedStreamReceived):
                    pushes += 1
                elif isinstance(event, (h2.events.StreamEnded, h2.events.StreamReset)):
                    body_complete = True
            if ttfb is None:
                ttfb = (time.monotonic() - start_time) * 1000.0
            if status <= 0:
                raise ConnectionError("h2 response missing :status")
            completed = True
            return status, resp_len, ttfb, pushes
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
# [B1] H2 池: 建连移出 per-key 锁; 满流快速失败
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
        def _configure(sock):
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
        ep_ctx = self._ssl_ctx_for(ep)
        reader, writer = await open_configured_connection(
            ep, self._args.timeout, ssl_ctx=ep_ctx, socket_config=_configure)
        conn = H2Connection(reader, writer, ep, self.group_of(ep), timeout=self._args.timeout)
        await conn.init()
        if self._stats is not None:
            self._stats.record_connect((time.perf_counter() - t0) * 1000.0)
        return conn

    async def get(self, ep) -> H2Connection:
        key = f"{ep.ip}:{ep.port}"
        if not self._health.ready(key): raise ConnectionError(f"endpoint {key} cooling down")
        picked = None
        dead = []
        async with self._lock_for(key):
            conns = self._conns.setdefault(key, [])
            # 清理死连接
            for c in conns:
                if c._closed or c.reader.at_eof():
                    dead.append(c)
            if dead:
                conns[:] = [c for c in conns if c not in dead]
            # 挑一个未满流的
            for c in conns:
                if c._active_streams < c._max_concurrent:
                    picked = c; break
            # [B1] 有连接但全部满流: 快速失败, 不等待
            if picked is None and conns:
                raise ConnectionError(f"h2 all streams busy on {key}")
        for c in dead:
            try: await c.close()
            except Exception: pass
        if picked is not None:
            return picked
        # [B1] 建连在锁外执行, 不阻塞同 key 其它 worker
        try:
            picked = await self._connect(ep)
            self._health.mark_ok(key)
        except Exception:
            self._health.mark_fail(key)
            raise
        # [FIX6] 并发建连防超限: 多个 worker 同时走到这里会各自建连, 若无条件追加,
        # 连接数将远超 h2_conns_per_ep。池满时丢弃多余新建, 复用空闲连接或按 [B1] 语义快速失败
        surplus = None
        reuse = None
        async with self._lock_for(key):
            conns = self._conns.setdefault(key, [])
            if len(conns) >= self._max_conns:
                surplus = picked
                reuse = next((c for c in conns
                              if not c._closed and c._active_streams < c._max_concurrent), None)
            else:
                conns.append(picked)
        if surplus is not None:
            try: await surplus.close()
            except Exception: pass
        if reuse is not None:
            return reuse
        if surplus is not None:
            raise ConnectionError(f"h2 all streams busy on {key}")
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
    if not all_eps: _warn("没有 h2 端点, h2 引擎无法工作"); return
    n = len(all_eps); base_extra = build_headers(args.header, args)

    hdr_variants = []
    for _ in range(64):
        if args.randomize:
            hdrs = random_header_set()
            for k, v in base_extra.items():
                if k.lower() not in ("user-agent", "accept-language", "cache-control", "pragma", "accept"):
                    hdrs[k] = v
        else:
            hdrs = dict(base_extra)
        hdr_variants.append(hdrs)

    while not stop_event.is_set():
        if scheduler and not await scheduler.acquire(): break
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
        stats.begin_request()
        try:
            conn = await pool.get(ep)
            status, resp_len, ttfb, pushes = await _run_timed(
                conn.send_request(path, hdrs, body_data), args.timeout)
            ms = (time.perf_counter() - start) * 1000.0
            ok = True if args.ignore_status else (status < 400 if status else False)
            stats.record(status=status, bytes_in=resp_len, ms=ms, ok=ok,
                         endpoint=ep.display, circuit_breaker=args.circuit_breaker,
                         stop_event=stop_event, h2_push=pushes, ttfb=ttfb)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # R1: 熔断冷却拦截 -> swallowed, 不 invalidate (连接未坏), 短暂退避
            if isinstance(e, ConnectionError) and "cooling down" in str(e):
                stats.note_swallow("breaker_open")
                await asyncio.sleep(0.01)
                continue
            ms = (time.perf_counter() - start) * 1000.0
            stats.record(status=0, ms=ms, ok=False, err=type(e).__name__,
                         endpoint=ep.display, circuit_breaker=args.circuit_breaker, stop_event=stop_event)
            if is_conn_level_error(e):
                try: await pool.invalidate_conn(ep, conn)
                except Exception: pass
        finally:
            stats.end_request()

# ===========================================================================
# HTTP/3 客户端 (B: StreamReset 处理 + TTFB)
# ===========================================================================
class H3Client:
    def __init__(self, ep, port: int, insecure: bool, timeout: float):
        self._ep = ep; self._port = port; self._insecure = insecure; self._timeout = timeout
        self._send_q: asyncio.Queue = asyncio.Queue(maxsize=1024)
        self._wakeup = asyncio.Event(); self._task: Optional[asyncio.Task] = None
        self._closed = False; self._connected = asyncio.Event(); self._conn_lock = asyncio.Lock()
        # 公开的 transmit 回调 (aioquic >=1.0 支持), 避免私有 API
        self._use_cb = hasattr(quic_connect, "transmit") or True

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
                    try: self._send_q.get_nowait()
                    except asyncio.QueueEmpty: break
                self._task = asyncio.create_task(self._run())
            if not self._connected.is_set():
                try: await asyncio.wait_for(self._connected.wait(), timeout=self._timeout)
                except asyncio.TimeoutError:
                    if self._task is None or self._task.done():
                        raise ConnectionError("h3 connect failed (task dead)")
                    raise
            if self._task is None or self._task.done(): raise ConnectionError("h3 connection unavailable")

    async def _run(self):
        config = QuicConfiguration(is_client=True, alpn_protocols=["h3"])
        if self._insecure: config.verify_mode = ssl.CERT_NONE
        pending: Dict[int, Tuple[asyncio.Queue, float]] = {}
        try:
            async with quic_connect(self._ep.ip, self._port, configuration=config,
                                     server_hostname=self._ep.hostname) as protocol:
                h3 = H3Connection(protocol._quic)  # aioquic 公开 API 中 H3Connection 绑定 quic
                self._connected.set()

                # 用回调替代忙轮询 (aioquic 1.x 提供 quic_receive_event 等内部事件则退回)
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
                        except Exception as e:
                            self._safe_put(resp_q, None)

                    protocol.transmit()

                    for event in h3.get_events():
                        sid = getattr(event, 'stream_id', None)
                        entry = pending.get(sid)
                        if entry is None: continue
                        q = entry[0]; self._safe_put(q, event)
                        # [B] StreamEnded/StreamReset 都终结 pending
                        if isinstance(event, (StreamEnded,)) or type(event).__name__ == "StreamReset":
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
        except Exception as e:
            _warn(f"h3 connection error on {self._ep.display}: {type(e).__name__}: {e}")
        finally:
            self._closed = True
            while True:
                try: self._send_q.get_nowait()
                except asyncio.QueueEmpty: break
            for q, _ in list(pending.values()): self._safe_put(q, None)
            self._connected.set()

    async def request(self, req_headers, body):
        await self._ensure_running()
        resp_q: asyncio.Queue = asyncio.Queue(maxsize=128)
        t0 = time.monotonic()
        try: await asyncio.wait_for(self._send_q.put((req_headers, body, resp_q)), timeout=self._timeout)
        except asyncio.TimeoutError: raise ConnectionError("h3 send queue full")
        self._wakeup.set()

        status = 0; body_len = 0; ttfb = None
        deadline = time.monotonic() + self._timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0: raise asyncio.TimeoutError()
            try: ev = await asyncio.wait_for(resp_q.get(), timeout=remaining)
            except asyncio.TimeoutError: raise
            if ev is None:
                raise ConnectionError("h3 connection closed before response completed")
            if isinstance(ev, HeadersReceived):
                if ttfb is None: ttfb = (time.monotonic() - t0) * 1000.0
                for k, v in ev.headers:
                    if k == b":status":
                        try: status = int(v.decode())
                        except (ValueError, UnicodeDecodeError): pass
            elif isinstance(ev, DataReceived):
                if ttfb is None: ttfb = (time.monotonic() - t0) * 1000.0
                body_len += len(ev.data)
            elif isinstance(ev, StreamEnded): break
        if ttfb is None: ttfb = (time.monotonic() - t0) * 1000.0
        if status <= 0:
            raise ConnectionError("h3 response missing :status")
        return status, body_len, ttfb

    def stop(self):
        self._closed = True; self._wakeup.set()
        if self._task is not None and not self._task.done(): self._task.cancel()


class H3Pool:
    def __init__(self, args, health):
        self._args = args; self._health = health
        self._clients: Dict[str, H3Client] = {}; self._locks: Dict[str, asyncio.Lock] = {}

    async def get(self, ep, port) -> H3Client:
        key = f"{ep.ip}:{port}"
        if not self._health.ready(key): raise ConnectionError(f"endpoint {key} cooling down")
        async with self._locks.setdefault(key, asyncio.Lock()):
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
    if not HAS_AIOQUIC: _error("HTTP/3 需要 aioquic: pip install aioquic"); return
    rnd = random.Random(); idx = 0; h3_eps = []
    for g in groups:
        for ep in g.endpoints:
            h3_port = ep.h3_port if ep.h3_port else ep.port
            if ep.scheme == "https": h3_eps.append((ep, g, h3_port))
    if not h3_eps: _warn("没有 h3 端点"); return
    n = len(h3_eps); base_extra = build_headers(args.header, args)

    while not stop_event.is_set():
        if scheduler and not await scheduler.acquire(): break
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
            if k.lower() not in ("host", "connection", "transfer-encoding", "content-length", "keep-alive"):
                req_headers.append((k, v))

        body_data = group.body
        if data_ctx and args.body: body_data = render_template(args.body, data_ctx).encode()
        if body_data: req_headers.append(("content-length", str(len(body_data))))

        start = time.perf_counter()
        stats.begin_request()
        try:
            client = await pool.get(ep, h3_port)
            status, resp_len, ttfb = await client.request(req_headers, body_data)
            ms = (time.perf_counter() - start) * 1000.0
            ok = True if args.ignore_status else (status < 400 if status else False)
            stats.record(status=status,
                         bytes_in=resp_len if not args.skip_body else 0, ms=ms, ok=ok,
                         endpoint=f"{ep.ip}:{h3_port} [h3]",
                         circuit_breaker=args.circuit_breaker, stop_event=stop_event, ttfb=ttfb)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # R1: 熔断冷却拦截 -> swallowed, 不 invalidate, 短暂退避
            if isinstance(e, ConnectionError) and "cooling down" in str(e):
                stats.note_swallow("breaker_open")
                await asyncio.sleep(0.01)
                continue
            ms = (time.perf_counter() - start) * 1000.0
            stats.record(status=0, ms=ms, ok=False, err=type(e).__name__,
                         endpoint=f"{ep.ip}:{h3_port} [h3]",
                         circuit_breaker=args.circuit_breaker, stop_event=stop_event)
            if is_conn_level_error(e):
                try: await pool.invalidate(ep, h3_port)
                except Exception: pass
        finally:
            stats.end_request()

# ===========================================================================
# [F6] WebSocket 引擎: 连接池复用
# ===========================================================================
def _ws_connect(ws_url, headers, timeout, ssl_ctx):
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


class WebSocketPool:
    """[F6] 按端点复用 WebSocket 连接, 最大 max_per_ep, 失效剔除"""
    def __init__(self, max_per_ep: int = 16):
        self._pools: Dict[str, List] = {}
        self._max = max_per_ep
        self._borrowed: Dict[str, int] = {}
        self._ssl_ctxs: Dict[Tuple[str, str], ssl.SSLContext] = {}

    def _ssl(self, hostname: str, insecure: bool):
        key = (hostname, insecure)
        if key not in self._ssl_ctxs:
            self._ssl_ctxs[key] = ServerDiscovery.make_ssl_ctx(hostname, insecure, None)
        return self._ssl_ctxs[key]

    def _borrow(self, key: str):
        self._borrowed[key] = self._borrowed.get(key, 0) + 1

    def _unborrow(self, key: str):
        cur = self._borrowed.get(key, 0) - 1
        if cur > 0: self._borrowed[key] = cur
        else: self._borrowed.pop(key, None)

    async def get(self, ws_url, key, hostname, insecure, base_extra, timeout):
        pool = self._pools.setdefault(key, [])
        # 1) 优先复用空闲连接
        while pool:
            ws = pool.pop()
            try:
                if ws.close_code is None:
                    self._borrow(key)
                    return ws, True
            except Exception:
                pass
            # 失效连接必须关闭, 否则 fd 泄漏
            try: await ws.close()
            except Exception: pass
        # 2) 并发上限按借用数严格约束 (原实现只看空闲池长度, max 形同虚设)
        if self._borrowed.get(key, 0) >= self._max:
            raise ConnectionError("ws pool busy")
        self._borrow(key)
        try:
            ssl_ctx = self._ssl(hostname, insecure) if insecure else None
            ws = await _ws_connect(ws_url, base_extra, timeout, ssl_ctx)
            await ws.__aenter__()
            return ws, False
        except Exception:
            self._unborrow(key)
            raise

    async def put(self, key, ws):
        self._unborrow(key)
        pool = self._pools.setdefault(key, [])
        if len(pool) < self._max:
            pool.append(ws)
        else:
            try: await ws.close()
            except Exception: pass

    async def discard(self, key, ws):
        """异常路径归还借用并关闭连接"""
        self._unborrow(key)
        try: await ws.close()
        except Exception: pass

    async def evict_all(self):
        for pool in self._pools.values():
            for ws in pool:
                try: await ws.close()
                except Exception: pass
        self._pools.clear(); self._borrowed.clear()


async def await_close(ws):
    try: await ws.close()
    except Exception: pass


async def ws_worker(groups, args, stats, stop_event, scheduler, data_pool=None):
    if not HAS_WEBSOCKETS:
        _error("WebSocket 引擎需要: pip install websockets")
        return
    rnd = random.Random(); idx = 0
    all_eps = [ep for g in groups for ep in g.endpoints]
    n = len(all_eps)
    if n == 0: return
    ep_group = build_ep_group_map(groups)
    base_extra = build_headers(args.header, args)
    wsp = WebSocketPool(max_per_ep=max(1, min(args.workers, 64)))

    try:
        while not stop_event.is_set():
            if scheduler and not await scheduler.acquire(): break
            ep = all_eps[rnd.randrange(n)] if args.mode == "random" else all_eps[idx % n]
            idx += 1
            group = ep_group.get(id(ep), groups[0])

            data_ctx = data_pool.get_next() if data_pool else None
            path = group.build_static_path()
            if data_ctx and args.format: path = render_template(args.format, data_ctx)

            ws_url = f"ws://{group.host_hdr}{path}"
            if ep.scheme == "https": ws_url = f"wss://{group.host_hdr}{path}"
            key = f"{ep.ip}:{ep.port}"

            start = time.perf_counter()
            stats.begin_request()
            ws = None; reused = False
            try:
                ws, reused = await wsp.get(ws_url, key, ep.hostname, args.insecure, base_extra, args.timeout)
                t0 = time.perf_counter()
                msg = "ping"
                if data_ctx and args.body:
                    msg = render_template(args.body, data_ctx)
                elif args.body:
                    msg = args.body.decode() if isinstance(args.body, bytes) else args.body

                await ws.send(msg)
                resp = await asyncio.wait_for(ws.recv(), timeout=args.timeout)
                ttfb = (time.perf_counter() - t0) * 1000.0
                resp_len = len(resp) if isinstance(resp, (bytes, str)) else 0
                ms = (time.perf_counter() - start) * 1000.0
                # 收到任意帧视为握手+应答成功; 异常走 except
                stats.record(status=101, bytes_in=resp_len, bytes_out=len(msg.encode() if isinstance(msg, str) else msg),
                             ms=ms, ok=True, endpoint=ep.display,
                             circuit_breaker=args.circuit_breaker, stop_event=stop_event, ttfb=ttfb)
                await wsp.put(key, ws)
                ws = None
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if ws is not None:
                    try: await wsp.discard(key, ws)
                    except Exception: pass
                if isinstance(e, ConnectionError) and "ws pool busy" in str(e):
                    # S1: 池并发上限背压: 不计入错误率, 短退避后继续
                    stats.note_swallow("ws_pool_busy")
                    await asyncio.sleep(0.005)
                    continue
                ms = (time.perf_counter() - start) * 1000.0
                stats.record(status=0, ms=ms, ok=False, err=type(e).__name__,
                             endpoint=ep.display, circuit_breaker=args.circuit_breaker, stop_event=stop_event)
            finally:
                stats.end_request()
    finally:
        await wsp.evict_all()

# ===========================================================================
# aiohttp 引擎 (TTFB via response 首字节时间近似: 用 read 前计时)
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
                if k.lower() not in ("user-agent", "accept-language", "cache-control", "pragma", "accept"):
                    hdrs[k] = v
        else:
            hdrs = dict(base_extra)
        hdr_variants.append(hdrs)

    while not stop_event.is_set():
        if scheduler and not await scheduler.acquire(): break
        ep = all_eps[rnd.randrange(n)] if args.mode == "random" else all_eps[idx % n]
        idx += 1
        group = ep_group.get(id(ep), groups[0])

        data_ctx = data_pool.get_next() if data_pool else None
        path = group.build_path() if args.randomize else group.build_static_path()
        if data_ctx and args.format: path = render_template(args.format, data_ctx)
        url = f"{group.scheme}://{group.host_hdr}{path}"
        hdrs = hdr_variants[rnd.randrange(len(hdr_variants))]

        body_data = group.body
        if data_ctx and args.body: body_data = render_template(args.body, data_ctx).encode()

        start = time.perf_counter()
        stats.begin_request()
        try:
            async with session.request(group.method, url, data=body_data, timeout=timeout,
                                       allow_redirects=False, auto_decompress=False, headers=hdrs) as resp:
                # aiohttp 返回 resp 即 header 已收 => TTFB 近似
                ttfb = (time.perf_counter() - start) * 1000.0
                if args.skip_body:
                    await resp.release(); length = 0
                else:
                    length = len(await resp.read())
                ms = (time.perf_counter() - start) * 1000.0
                ok = True if args.ignore_status else resp.status < 400
                stats.record(status=resp.status, bytes_in=length, ms=ms, ok=ok,
                             endpoint=ep.display, circuit_breaker=args.circuit_breaker,
                             stop_event=stop_event, ttfb=ttfb)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            ms = (time.perf_counter() - start) * 1000.0
            stats.record(status=0, ms=ms, ok=False, err=type(e).__name__,
                         endpoint=ep.display, circuit_breaker=args.circuit_breaker, stop_event=stop_event)
        finally:
            stats.end_request()


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
    force_close = bool(getattr(args, "no_keepalive", False))
    kw = dict(limit=args.workers * 2, limit_per_host=args.workers,
              enable_cleanup_closed=True, force_close=force_close, ttl_dns_cache=300, use_dns_cache=True, ssl=ssl_ctx)
    if not force_close:
        kw["keepalive_timeout"] = args.keepalive_timeout
    try:
        return aiohttp.TCPConnector(read_bufsize=args.read_bufsize, **kw)
    except TypeError:
        kw.pop("ssl", None)
        return aiohttp.TCPConnector(**kw)

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
            elif key == "p999": actual = getattr(stats, "p999", 0.0)
            elif key == "ttfb_p99":
                actual = getattr(stats, "ttfb_p99", 0.0)
            elif key == "error_rate": actual = stats.error_rate
            elif key == "rps": actual = stats.requests / max(elapsed, 0.001)
            else: continue

            if val.endswith('ms'): target = float(val[:-2])
            elif val.endswith('s'): target = float(val[:-1]) * 1000
            elif val.endswith('%'): target = float(val[:-1]) / 100.0
            else: target = float(val)

            # [v24] 单位陷阱提示: status_xxx 用百分比但漏了 %
            if key.startswith("status_") and not val.endswith(('%', 'ms', 's')):
                _warn(f"threshold '{key}{op}{val}': status_* 指标是比例(0~1), 建议写成 {val}%")

            if op == "<" and not (actual < target): return False
            if op == "<=" and not (actual <= target): return False
            if op == ">" and not (actual > target): return False
            if op == ">=" and not (actual >= target): return False
        except Exception as e:
            _warn(f"threshold parse failed for '{key}{op}{val}': {e}")
    return True

# ===========================================================================
# [F10] HTML 报告: 全量 html.escape
# ===========================================================================
def generate_html_report(stats: Stats, elapsed: float, filename: str):
    """Generate a self-contained HTML report with no external JS/CDN dependencies."""
    def esc(v):
        return html_mod.escape(str(v))

    status_rows = "".join(
        f"<tr><td>{esc(code)}</td><td>{count:,}</td><td>{count / max(stats.requests, 1) * 100.0:.3f}%</td></tr>"
        for code, count in sorted(stats.status_codes.items())
    ) or '<tr><td colspan="3">无状态码样本</td></tr>'
    lat_map = {round(t, 3): (p50, p99) for t, p50, p99 in stats._timeline_lat}
    timeline_rows = "".join(
        f"<tr><td>{t:.1f}</td><td>{r:.2f}</td><td>{lat_map.get(round(t,3), (0.0,0.0))[0]:.2f}</td><td>{lat_map.get(round(t,3), (0.0,0.0))[1]:.2f}</td></tr>"
        for t, r in list(stats._timeline_rps)[-300:]
    ) or '<tr><td colspan="4">无时间序列样本</td></tr>'
    error_rows = "".join(
        f"<tr><td>{esc(k)}</td><td>{v}</td></tr>" for k, v in sorted(stats.errors.items())
    ) or '<tr><td colspan="2">无错误</td></tr>'
    html_content = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Termux HTTP Stress v{esc(VERSION)} Report</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:24px;line-height:1.45;background:#f5f7fa;color:#1f2937}}
.wrap{{max-width:1400px;margin:auto}}h1{{margin-bottom:6px}}.muted{{color:#6b7280}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:18px 0}}
.card{{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:16px}}.value{{font-size:26px;font-weight:700}}
table{{width:100%;border-collapse:collapse;background:#fff;border:1px solid #e5e7eb}}th,td{{padding:9px;border-bottom:1px solid #eee;text-align:left}}th{{background:#f9fafb}}
.section{{margin-top:24px}}code{{background:#eef2ff;padding:2px 5px;border-radius:4px}}
</style></head><body><div class="wrap">
<h1>压测报告 v{esc(VERSION)}</h1><div class="muted">Scenario IR: <code>{esc(getattr(stats,'scenario_digest','n/a'))}</code></div>
<div class="cards">
<div class="card"><div>Requests</div><div class="value">{stats.requests:,}</div></div>
<div class="card"><div>Success</div><div class="value">{stats.success:,}</div></div>
<div class="card"><div>Failed</div><div class="value">{stats.failed:,}</div></div>
<div class="card"><div>Avg RPS</div><div class="value">{stats.requests/max(elapsed,0.001):.1f}</div></div>
<div class="card"><div>P99</div><div class="value">{stats.p99:.2f} ms</div></div>
<div class="card"><div>P99 TTFB</div><div class="value">{stats.ttfb_p99:.2f} ms</div></div>
<div class="card"><div>Error Rate</div><div class="value">{stats.error_rate*100:.3f}%</div></div>
<div class="card"><div>Client state</div><div class="value">{esc(getattr(stats,'client_classification','UNKNOWN'))}</div></div>
</div>
<div class="section"><h2>Runtime Telemetry</h2><table><tr><th>指标</th><th>最大值</th></tr>
<tr><td>CPU / host capacity</td><td>{getattr(stats,'runtime_cpu_max',0.0):.1f}%</td></tr>
<tr><td>RSS</td><td>{getattr(stats,'runtime_rss_max_mb',0.0):.1f} MB</td></tr>
<tr><td>FD</td><td>{getattr(stats,'runtime_fd_max',0)}</td></tr>
<tr><td>Scheduler lag</td><td>{getattr(stats,'runtime_scheduler_lag_max_ms',0.0):.2f} ms</td></tr>
</table></div>
<div class="section"><h2>HTTP Status</h2><table><tr><th>Status</th><th>Count</th><th>Ratio</th></tr>{status_rows}</table></div>
<div class="section"><h2>Timeline</h2><table><tr><th>t(s)</th><th>RPS</th><th>P50(ms)</th><th>P99(ms)</th></tr>{timeline_rows}</table></div>
<div class="section"><h2>Errors</h2><table><tr><th>Error</th><th>Count</th></tr>{error_rows}</table></div>
</div></body></html>"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"[INFO] HTML 报告已生成: {filename}")
    except Exception as e:
        _warn(f"生成 HTML 报告失败: {e}")


def final_report(stats: Stats, elapsed: float, groups):
    # [v25] Rust engine: surface the Rust core's own (precise) report.
    if _RUST_REPORT:
        rep = _RUST_REPORT
        print("\n" + "=" * 60 + "\nSUMMARY (Rust data-plane)\n" + "=" * 60)
        print(f"Duration         : {elapsed:.2f}s")
        print(f"Total requests   : {rep.get('requests', 0)}")
        if elapsed > 0:
            print(f"Avg RPS          : {rep.get('requests', 0) / elapsed:.2f}")
        print(f"Success          : {rep.get('success', 0)}")
        print(f"Failed           : {rep.get('failed', 0)}")
        print(f"Error rate       : {rep.get('error_rate', 0):.6f}")
        if rep.get("bytes_in"):
            print(f"Bytes in         : {rep.get('bytes_in'):,}")
        if rep.get("connect_count"):
            print(f"New connections  : {rep.get('connect_count')}")
        if rep.get("p50_ms") is not None:
            print(f"P50 latency      : {rep.get('p50_ms'):.3f}ms")
        if rep.get("p95_ms") is not None:
            print(f"P95 latency      : {rep.get('p95_ms'):.3f}ms")
        if rep.get("p99_ms") is not None:
            print(f"P99 latency      : {rep.get('p99_ms'):.3f}ms")
        if rep.get("p999_ms") is not None:
            print(f"P99.9 latency    : {rep.get('p999_ms'):.3f}ms")
        if rep.get("min_us") is not None:
            print(f"Min/Max latency  : {rep.get('min_us')/1000:.3f} / {rep.get('max_us',0)/1000:.3f}ms")
        if rep.get("ttfb_p50_ms") is not None:
            print(f"TTFB P50/P99     : {rep.get('ttfb_p50_ms'):.3f} / {rep.get('ttfb_p99_ms',0):.3f}ms")
        print(f"Peak in-flight   : {rep.get('peak_in_flight', 0)}")
        if rep.get("dropped_tokens"):
            print(f"Dropped tokens   : {rep.get('dropped_tokens')}")
        if rep.get("status"):
            print("\nHTTP status codes:")
            for code in sorted(rep['status'], key=lambda x: int(x)):
                print(f"  {code}: {rep['status'][code]}")
        if rep.get("errors"):
            print("\nErrors:")
            for err in sorted(rep['errors']):
                print(f"  {err}: {rep['errors'][err]}")
        print(f"Breaker (open/half/closed): {rep.get('breaker_open',0)}/{rep.get('breaker_half',0)}/{rep.get('breaker_closed',0)}")
        print("=" * 60)
        return
    print("\n" + "=" * 60 + "\nSUMMARY\n" + "=" * 60)
    print(f"Duration         : {elapsed:.2f}s")
    print(f"Total requests   : {stats.requests}")
    if elapsed > 0: print(f"Avg RPS          : {stats.requests / elapsed:.2f}")
    print(f"Success          : {stats.success}\nFailed           : {stats.failed}\nError rate       : {stats.error_rate:.4f}")
    print(f"Bytes in         : {stats.bytes_in:,}\nBytes out        : {stats.bytes_out:,}")
    if stats.truncated_responses:
        print(f"Truncated body   : {stats.truncated_responses} responses hit drain cap")
    if stats.h2_pushes > 0:
        print(f"H2 Server Pushes : {stats.h2_pushes}")
    if stats.connect_count > 0:
        print(f"Avg connect time : {stats.connect_ms_sum / stats.connect_count:.2f}ms (over {stats.connect_count} new conns)")
    print(f"Peak in-flight   : {stats.peak_in_flight}")
    print(f"Scenario IR      : {getattr(stats, 'scenario_digest', '') or 'n/a'}")
    print(f"Client telemetry : {getattr(stats, 'client_classification', 'UNKNOWN')} | CPUmax={getattr(stats, 'runtime_cpu_max', 0.0):.1f}% | RSSmax={getattr(stats, 'runtime_rss_max_mb', 0.0):.1f}MB | FDmax={getattr(stats, 'runtime_fd_max', 0)} | sched_lag={getattr(stats, 'runtime_scheduler_lag_max_ms', 0.0):.1f}ms")
    if stats.requests > 0:
        print(f"Avg latency      : {stats.lat_sum / stats.requests:.2f}ms")
        if stats.lat_min is not None: print(f"Min latency      : {stats.lat_min:.2f}ms")
        print(f"Max latency      : {stats.lat_max:.2f}ms\nP50 latency      : {stats.p50:.2f}ms\nP95 latency      : {stats.p95:.2f}ms\nP99 latency      : {stats.p99:.2f}ms\nP999 latency     : {stats.p999:.2f}ms")
        if stats.ttfb_min is not None:
            print(f"TTFB min/avg/max : {stats.ttfb_min:.2f} / {stats.ttfb_sum/stats.requests:.2f} / {stats.ttfb_max:.2f}ms")
            print(f"TTFB P50/P99     : {stats.ttfb_p50:.2f} / {stats.ttfb_p99:.2f}ms")
    if stats.status_codes:
        print("\nHTTP status codes:")
        for code in sorted(stats.status_codes.keys()): print(f"  {code}: {stats.status_codes[code]}")
    if stats.errors:
        print("\nErrors:")
        for err in sorted(stats.errors.keys()): print(f"  {err}: {stats.errors[err]}")
    if stats.swallowed:
        print("\nSwallowed exceptions (diagnostics):")
        for err in sorted(stats.swallowed.keys()): print(f"  {err}: {stats.swallowed[err]}")
    if stats._per_endpoint:
        print("\nPer-endpoint requests:")
        for ep, count in sorted(stats._per_endpoint.items(), key=lambda x: -x[1]): print(f"  {ep}: {count}")
    print("=" * 60)


def print_cluster_summary(agg: ClusterAggregator, elapsed: float):
    t = agg.totals()
    missing = agg.missing()
    print("\n" + "=" * 60 + "\nCLUSTER SUMMARY\n" + "=" * 60)
    print(f"Workers reporting: {len(agg.latest)}")
    if missing:
        print(f"*** WARNING: {len(missing)} worker(s) missing final snapshot: {sorted(map(str, missing))}")
        print(f"*** 汇总数据可能不完整 (数据完整性告警)")
    if agg.timed_out:  # [FIX3] 汇总中明确标注被心跳超时剔除的 worker
        print(f"*** WARNING: {len(agg.timed_out)} worker(s) dropped by heartbeat timeout (其部分数据已从汇总剔除): {sorted(map(str, agg.timed_out))}")
    print(f"Duration         : {elapsed:.2f}s")
    print(f"Total requests   : {t['req']}")
    if elapsed > 0: print(f"Avg RPS          : {t['req'] / elapsed:.2f}")
    print(f"Success          : {t['ok']}\nFailed           : {t['fail']}")
    err = t['fail'] / t['req'] if t['req'] else 0.0
    print(f"Error rate       : {err:.4f}")
    print(f"Bytes in         : {t['bytes_in']:,}\nBytes out        : {t['bytes_out']:,}")
    if t['truncated']: print(f"Truncated body   : {t['truncated']}")
    if t['h2_pushes']: print(f"H2 Server Pushes : {t['h2_pushes']}")
    if t['connect_count'] > 0:
        print(f"Avg connect time : {t['connect_ms_sum'] / t['connect_count']:.2f}ms (over {t['connect_count']} new conns)")
    print(f"Peak in-flight   : {t['peak_in_flight']}")
    if t['count'] > 0:
        print(f"Avg latency      : {t['lat_sum']/t['count']:.2f}ms")
        if t['lat_min'] is not None: print(f"Min latency      : {t['lat_min']:.2f}ms")
        print(f"Max latency      : {t['lat_max']:.2f}ms")
        print(f"P50 latency      : {merged_percentile(t['hist'], t['count'], 50):.2f}ms")
        print(f"P95 latency      : {merged_percentile(t['hist'], t['count'], 95):.2f}ms")
        print(f"P99 latency      : {merged_percentile(t['hist'], t['count'], 99):.2f}ms")
        print(f"P999 latency     : {merged_percentile(t['hist'], t['count'], 99.9):.2f}ms")
        if t['ttfb_min'] is not None:
            print(f"TTFB P99         : {merged_percentile(t['ttfb_hist'], t['count'], 99):.2f}ms")
    if t['status_codes']:
        print("\nHTTP status codes:")
        for code in sorted(t['status_codes']): print(f"  {code}: {t['status_codes'][code]}")
    print("=" * 60)


# ===========================================================================
# [F11] fd 上限: resource + shell ulimit 双路径 + 预检
# ===========================================================================
def raise_fd_limit() -> int:
    limit = 1024
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limit = soft
        want = min(hard, 1048576)
        if soft < want:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
                limit = want
                _info(f"RLIMIT_NOFILE: {soft} -> {want}")
            except (ValueError, OSError) as e:
                # [F11] shell 层尝试 (Termux)
                try:
                    if sys.platform == "linux":
                        os.system(f"ulimit -n {want} 2>/dev/null")
                except Exception:
                    pass
                _warn(f"Cannot raise RLIMIT_NOFILE ({e}); soft={soft}. 高并发可能 'Too many open files'.")
    except ImportError:
        _info("resource 模块不可用; 请手动 'ulimit -n 65536'")
    return limit

# ===========================================================================
# [F8] 信号处理: loop.add_signal_handler + 二次 Ctrl+C 强退
# ===========================================================================
def setup_signal_handlers(stop_event, loop=None):
    state = {"count": 0}
    def _force():
        _warn("第二次 Ctrl+C: 强制退出")
        os._exit(130)

    def _handler(signum, frame):
        state["count"] += 1
        if state["count"] >= 2:
            os._exit(130)
        stop_event.set()

    # [FIX2] 原代码误写 hasattr(asyncio, "add_signal_handler") 恒为 False, loop 路径从未生效过;
    # 改为检查 loop 对象本身, 激活 [F8] 设计的主路径
    if loop is not None and hasattr(loop, "add_signal_handler"):
        try:
            # [FIX2] 第一次置 stop_event; 第二次强制退出 (计数语义与 signal 回退路径一致)
            def _int_once():
                state["count"] += 1
                if state["count"] >= 2:
                    _force()
                    return
                stop_event.set()
            loop.add_signal_handler(signal.SIGINT, _int_once)
            loop.add_signal_handler(signal.SIGTERM, _int_once)
            return ("loop", None, None)
        except (NotImplementedError, RuntimeError):
            pass
    old_int = signal.signal(signal.SIGINT, _handler)
    old_term = signal.signal(signal.SIGTERM, _handler)
    return ("signal", old_int, old_term)

def restore_signal_handlers(kind, old_int, old_term):
    if kind == "signal":
        try: signal.signal(signal.SIGINT, old_int)
        except (ValueError, OSError): pass
        try: signal.signal(signal.SIGTERM, old_term)
        except (ValueError, OSError): pass


# ===========================================================================
# [F7] 数据池: 读失败快速失败; 无状态轮转
# ===========================================================================
class DataPool:
    def __init__(self, filepath: str):
        self.data = []
        self._idx = 0
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.data = [row for row in reader]
            if not self.data:
                _error(f"CSV 数据文件为空: {filepath}")
                sys.exit(2)
            print(f"[INFO] 加载 {len(self.data)} 条 CSV 数据记录")
        except FileNotFoundError:
            _error(f"[F7] CSV 文件不存在, 快速失败: {filepath}")
            sys.exit(2)
        except Exception as e:
            _error(f"[F7] 读取 CSV 失败, 快速失败: {e}")
            sys.exit(2)

    def get_next(self) -> Optional[dict]:
        if not self.data: return None
        item = self.data[self._idx % len(self.data)]
        self._idx += 1
        return item

# ===========================================================================
# 统一引擎解析：explicit fail-fast / auto fallback
# ===========================================================================
def _canonical_group_target(g):
    path = getattr(g, "base_path", "/") or "/"
    if not path.startswith("/"):
        path = "/" + path
    scheme = getattr(g, "scheme", "http")
    host = getattr(g, "hostname", "")
    port = getattr(g, "port", 0)
    default_port = 443 if scheme == "https" else 80
    authority = host if port == default_port else f"{host}:{port}"
    return f"{scheme}://{authority}{path}"

def _preflight_engine(args, has_tcp):
    """Validate engine prerequisites under a universal parameter contract.

    Every engine accepts the common CLI contract. Protocol- or transport-specific
    controls that have no valid meaning are neutralized and reported rather than
    rejected as false compatibility failures.
    """
    engine = args.engine
    deps = {
        "aiohttp": (HAS_AIOHTTP, "aiohttp"),
        "h2": (HAS_H2, "h2"),
        "h3": (HAS_AIOQUIC, "aioquic"),
        "ws": (HAS_WEBSOCKETS, "websockets"),
    }
    if engine in deps and not deps[engine][0]:
        raise ValueError(f"显式 --engine {engine} 但依赖 {deps[engine][1]} 不可用；显式引擎禁止静默降级")
    if engine == "rust" and _cargo_find() is None:
        raise ValueError("显式 --engine rust 但当前环境没有 cargo；Rust 引擎禁止静默降级")
    return engine

def _resolve_engine(args, groups, has_tcp):
    """Resolve one engine after discovery; auto may fallback, explicit may not."""
    requested = args.engine
    if requested != "auto":
        # Scenario capability is only fully knowable after discovery.
        if requested == "rust":
            ok, why = _rust_scenario_supported(args, [_canonical_group_target(g) for g in groups])
            if not ok:
                raise ValueError(f"rust 引擎当前不支持该场景: {why}")
        return requested
    if has_tcp:
        _info("[INFO] 自动选择引擎: raw (TCP 干扰要求 raw data-plane)")
        return "raw"

    targets_for_rust = [_canonical_group_target(g) for g in groups]
    rust_ok, rust_reason = _rust_scenario_supported(args, targets_for_rust)
    rust_ok = rust_ok and _cargo_find() is not None and not args.dry_run and args.duration > 0
    if rust_ok and getattr(args, "prefer_rust", False):
        _info("[INFO] 自动选择引擎: rust (场景可 1:1 映射)")
        return "rust"
    if rust_reason:
        _info(f"[INFO] rust 跳过: {rust_reason}")

    has_h2 = any(ep.alpn == "h2" for g in groups for ep in g.endpoints)
    has_h3 = any(ep.h3_port > 0 for g in groups for ep in g.endpoints)
    if getattr(args, "prefer_h3", False) and has_h3 and HAS_AIOQUIC:
        _info("[INFO] 自动选择引擎: h3 (因 --prefer-h3)")
        return "h3"
    if has_h2 and HAS_H2:
        _info("[INFO] 自动选择引擎: h2")
        return "h2"
    if HAS_AIOHTTP:
        _info("[INFO] 自动选择引擎: aiohttp")
        return "aiohttp"
    _info("[INFO] 自动选择引擎: raw")
    return "raw"

# ===========================================================================
# 通用压测核心
# ===========================================================================
async def run_load(args, norm_targets, body, engine, has_tcp, stop_event,
                   on_report=None, enable_export=False):
    if getattr(args,"body_mode","count") == "discard": args.skip_body = True
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
            resolved = await ServerDiscovery.resolve_dns(hostname, use_aiodns=HAS_AIODNS)
            if not resolved:
                resolved = [(hostname, ':' in hostname)]
            for ip, is_v6 in resolved:
                group.endpoints.append(Endpoint(ip=ip, port=port, hostname=hostname, scheme=scheme,
                                                is_v6=is_v6, alpn="http/1.1"))
            groups.append(group)
        else:
            group = await ServerDiscovery.discover(target_url, args.path, args.method, body, args.insecure, args.discovery_timeout, prefer_ips)
            groups.append(group)

    _info(f"\n[INFO] 发现完成: {len(groups)} 组目标, {sum(len(g.endpoints) for g in groups)} 个端点")

    engine = _resolve_engine(args, groups, has_tcp)
    if args.benchmark == "single-core" and engine != "rust":
        raise ValueError("--benchmark single-core 无法获得 native Rust 引擎；请安装 Cargo 并确保场景可被 Rust 1:1 映射")
    scenario = build_scenario_ir(args, norm_targets, body or b"", engine)
    _info(f"[SCENARIO] IR digest={scenario.digest} engine={engine}")

    scheduler = None
    if args.rps > 0 or args.rate_mode == "arrival":
        scheduler = Scheduler(args.rps, mode=args.rate_mode, stop_event=stop_event,
                              burst=args.rps_burst, queue_size=args.arrival_queue,
                              drop_policy=args.arrival_drop_policy)
        _info(f"[INFO] 调度器: {args.rate_mode} mode, rate={args.rps}, drop_policy={args.arrival_drop_policy}")

    ssl_ctx = None
    if any(g.scheme == "https" for g in groups):
        alpn = ["h2", "http/1.1"] if engine in ("h2", "auto") else None
        ssl_ctx = ServerDiscovery.make_ssl_ctx("", args.insecure, alpn)

    if args.dry_run:
        scheduler = OneShotScheduler(stop_event)

    tcp_intf = TCPInterference(args)
    health = EndpointHealth(cooldown=args.ep_cooldown,
                            failure_threshold=args.ep_breaker,
                            recovery_timeout=args.ep_recovery)
    stats = Stats()
    stats.scenario_digest = scenario.digest
    telemetry = RuntimeTelemetry(args.runtime_telemetry_interval)
    telemetry_task = asyncio.create_task(telemetry.run(stop_event))

    if enable_export and args.export:
        try:
            ext = args.export; filename = f"{args.export_file}.{ext}"
            stats.enable_detail_log(filename, fmt=ext, sample=args.export_sample)
            _info(f"详情流式导出: {filename} (sample=1/{args.export_sample})")
        except OSError as e:
            _warn(f"无法打开导出文件 ({e}); 导出已禁用"); args.export = None

    h2_pool = H2Pool(groups, args, health, stats) if engine == "h2" else None
    h3_pool = H3Pool(args, health) if engine == "h3" else None

    workers = []; session = None; connector = None

    if engine == "rust":
        # Embedded Rust data-plane: a single subprocess that internally spawns
        # `workers` threads (per-core, GIL-free). Run in a thread so the asyncio
        # loop stays responsive for signals/shutdown. Sets _RUST_REPORT on success.
        loop = asyncio.get_event_loop()
        hp_targets = [g.target if hasattr(g, "target") else (f"{g.scheme}://{g.hostname}:{g.port}") for g in groups]
        try:
            await loop.run_in_executor(None, _run_rust_engine, args, hp_targets)
            engine = "rust"
        except Exception as e:
            if args.engine == "rust":
                _error(f"[rust] 引擎启动/运行失败: {e}; 显式 Rust 模式拒绝静默降级")
                raise
            _error(f"[rust] 引擎启动/运行失败: {e} -> 降级到 raw 引擎")
            engine = "raw"

    # [v25] Rust engine already did the load in a subprocess; return the shared
    # (empty) stats — final_report reads the global _RUST_REPORT instead.
    if engine == "rust":
        telemetry_task.cancel()
        try: await telemetry_task
        except asyncio.CancelledError: pass
        stats.runtime_cpu_max = telemetry.max_cpu_one_core_pct
        stats.runtime_rss_max_mb = telemetry.max_rss_mb
        stats.runtime_fd_max = telemetry.max_fd
        stats.runtime_scheduler_lag_max_ms = telemetry.max_scheduler_lag_ms
        stats.client_classification = telemetry.classification(stats, raise_fd_limit())
        return stats, groups

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
        worker_args = (groups, args, ssl_ctx, tcp_intf, health, stats, stop_event, scheduler, data_pool)

    if args.dry_run:
        _info("[INFO] Dry-run: 每个启动 worker 仅发送一次请求以验证连通性...")
        workers.append(asyncio.create_task(worker_func(*worker_args)))
        await asyncio.gather(*workers, return_exceptions=True)
        await _teardown(h2_pool, h3_pool, session, connector)
        try:
            telemetry_task.cancel(); await telemetry_task
        except asyncio.CancelledError: pass
        stats.runtime_cpu_max = telemetry.max_cpu_one_core_pct
        stats.runtime_rss_max_mb = telemetry.max_rss_mb
        stats.runtime_fd_max = telemetry.max_fd
        stats.runtime_scheduler_lag_max_ms = telemetry.max_scheduler_lag_ms
        stats.client_classification = telemetry.classification(stats, raise_fd_limit())
        stats.close_detail()
        return stats, groups

    def make_worker(): return worker_func(*worker_args)

    initial_workers = args.workers
    if args.ramp_up > 0: initial_workers = max(1, args.workers // 10)

    for i in range(max(1, initial_workers)):
        workers.append(asyncio.create_task(supervised(f"{engine}-w{i}", make_worker, stop_event, stats)))

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
                    workers.append(asyncio.create_task(supervised(f"{engine}-r{len(workers)}", make_worker, stop_event, stats)))
                _info(f"[RAMP-UP] {elapsed:.0f}s: {len(workers)} workers")
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
        if args.warmup > 0:
            stats._recording = False
            _info(f"[INFO] 预热 {args.warmup}s (请求不计入统计)...")
            try: await asyncio.wait_for(stop_event.wait(), timeout=args.warmup)
            except asyncio.TimeoutError: pass
            if not stop_event.is_set():
                stats.reset()
                _info("[INFO] 预热完成, 正式统计开始")

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
        # [F3] 背压指标入 stats
        if scheduler and hasattr(scheduler, "metrics"):
            sm = scheduler.metrics()
            stats.dropped_tokens = sm.get("dropped_tokens", 0)
            stats.blocked_ms = sm.get("blocked_ms", 0.0)
        drain_s = getattr(args, "drain_timeout", 5.0)
        _info(f"[INFO] 优雅退出: 等待在途请求完成 (最多 {drain_s:.0f}s)...")
        if workers:
            done, pending = await asyncio.wait(workers, timeout=drain_s)
            for w in pending: w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
        if reporter_task:
            reporter_task.cancel()
            try: await reporter_task
            except asyncio.CancelledError: pass
        try:
            telemetry_task.cancel()
            await telemetry_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        try:
            stats.runtime_cpu_max = telemetry.max_cpu_pct
            stats.runtime_rss_max_mb = telemetry.max_rss_mb
            stats.runtime_fd_max = telemetry.max_fd
            stats.runtime_scheduler_lag_max_ms = telemetry.max_scheduler_lag_ms
            fd_limit_now = raise_fd_limit()
            stats.client_classification = telemetry.classification(stats, fd_limit_now)
        except Exception:
            pass
        await _teardown(h2_pool, h3_pool, session, connector)
        stats.close_detail()
    return stats, groups


async def _teardown(h2_pool, h3_pool, session, connector):
    if h2_pool is not None: await h2_pool.close_all()
    if h3_pool is not None: await h3_pool.close_all()
    if session: await session.close()
    if connector: await connector.close()

# ===========================================================================
# 单进程模式
# ===========================================================================
async def _run_single_process(args, norm_targets, body, engine, has_tcp):
    gc.disable(); gc.set_threshold(50000, 10, 10)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    kind, old_int, old_term = setup_signal_handlers(stop_event, loop)

    state = {"last_req": 0, "last_time": time.monotonic(), "last_bytes": 0}

    def on_report(stats):
        now = time.monotonic()
        elapsed = now - stats.start_mono
        if not stats._recording:
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
              f"rps={rps:.1f} inflight={stats.in_flight} peak_if={stats.peak_in_flight} "
              f"p50={stats.p50:.1f} p99={stats.p99:.1f} ttfb_p99={stats.ttfb_p99:.1f}ms "
              f"bw={bw/1024:.1f}KB/s status=[{status_snap}]", flush=True)
        stats.record_timeline(elapsed, rps, stats.p50, stats.p99)
        state.update(last_req=stats.requests, last_time=now, last_bytes=stats.bytes_in)

    try:
        stats, groups = await run_load(args, norm_targets, body, engine, has_tcp, stop_event,
                                       on_report=on_report, enable_export=True)
        elapsed = time.monotonic() - stats.start_mono
        final_report(stats, elapsed, groups)
        if args.report_html: generate_html_report(stats, elapsed, args.report_html)
        if stats.dropped_tokens:
            _warn(f"arrival 调度器丢弃令牌 {stats.dropped_tokens} (blocked {stats.blocked_ms:.0f}ms)")

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
    except (ValueError, RuntimeError) as exc:
        _error(str(exc))
        return 2
    finally:
        gc.enable()
        restore_signal_handlers(kind, old_int, old_term)


# ===========================================================================
# 本地多进程
# ===========================================================================
def worker_process_main(args_dict, norm_targets, body, engine, has_tcp, worker_id, stats_q, mp_stop):
    global LOG_FORMAT
    LOG_FORMAT = args_dict.get("log_format", "text")
    VERBOSE = (worker_id == 0)

    gc.disable(); gc.set_threshold(50000, 10, 10)
    args = argparse.Namespace(**args_dict)

    if args.processes > 1 and args.rps > 0:
        args.rps = args.rps / args.processes
    if getattr(args, "start_delay", 0.0) > 0:
        deadline = getattr(args, "_start_at", None)
        if deadline is not None:
            remain = deadline - time.monotonic()
            if remain > 0:
                time.sleep(remain)

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
    loop = asyncio.get_running_loop()
    kind, old_int, old_term = setup_signal_handlers(stop_event, loop)

    async def _stop_proxy():
        while not mp_stop.is_set():
            await asyncio.sleep(0.2)
        stop_event.set()
    proxy_task = asyncio.create_task(_stop_proxy())

    def on_report(stats):
        try:
            stats_q.put_nowait(stats.snapshot(wid=worker_id))
        except (queue_mod.Full, OSError):
            pass

    try:
        stats, groups = await run_load(args, norm_targets, body, engine, has_tcp, stop_event, on_report=on_report)
        try:
            stats_q.put(stats.snapshot(wid=worker_id, final=True), timeout=1.0)
        except (queue_mod.Full, OSError):
            pass
    finally:
        proxy_task.cancel()
        restore_signal_handlers(kind, old_int, old_term)


# ===========================================================================
# 本地多进程 Master
# ===========================================================================
def master_loop(args, norm_targets, body, engine, has_tcp):
    mp_stop = multiprocessing.Event()
    stats_q = multiprocessing.Queue(maxsize=max(32, args.processes * 8))
    args._start_at = time.monotonic() + max(0.0, float(getattr(args, "start_delay", 0.0)))
    args_dict = vars(args)

    agg = ClusterAggregator()
    procs = []
    for i in range(args.processes):
        agg.register(i)
        procs.append(multiprocessing.Process(
            target=worker_process_main,
            args=(args_dict, norm_targets, body, engine, has_tcp, i, stats_q, mp_stop)))
    for pr in procs:
        pr.start()

    start = agg.start_time
    last_print = start; last_req = 0
    total_run = args.duration + args.warmup if args.duration > 0 else None
    hard_deadline = (start + total_run + args.drain_timeout + 120) if total_run else None
    exit_code = 0

    try:
        while True:
            try:
                while True:
                    snap = stats_q.get(timeout=0.2)
                    agg.apply(snap)
            except queue_mod.Empty:
                pass

            now = time.monotonic()
            if now - last_print >= args.interval:
                t = agg.totals()
                rps = (t["req"] - last_req) / max(now - last_print, 1e-6)
                print(f"[Master {int(now-start):4d}s] procs={args.processes} req={t['req']} ok={t['ok']} "
                      f"fail={t['fail']} rps={rps:.1f} p99={merged_percentile(t['hist'], t['count'], 99):.1f}ms", flush=True)
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

    elapsed = time.monotonic() - agg.start_time
    print_cluster_summary(agg, elapsed)
    if args.threshold:
        view = MergedStatsView(agg.totals())
        if check_thresholds(view, parse_thresholds(args.threshold), elapsed):
            print("[INFO] 阈值断言: PASSED")
        else:
            print("[ERROR] 阈值断言: FAILED"); exit_code = 2
    return exit_code

# ===========================================================================
# [F2] 远程分布式: 认证 (HMAC token / 可插拔 TLS) + 心跳 + 失联回收
# ===========================================================================
def _make_token(secret: bytes, wid: str) -> str:
    return hmac.new(secret, wid.encode(), hashlib.sha256).hexdigest()


def _check_token(secret: bytes, wid: str, token: str) -> bool:
    expect = _make_token(secret, wid)
    return hmac.compare_digest(expect, token)


async def remote_master_main(args):
    agg = ClusterAggregator()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    kind, old_int, old_term = setup_signal_handlers(stop, loop)

    secret = args.master_secret.encode() if args.master_secret else None
    require_auth = secret is not None
    heartbeat_timeout = args.master_heartbeat_timeout
    last_seen: Dict[str, float] = {}

    async def _handle(reader, writer):
        peer = writer.get_extra_info("peername")
        wid = None
        try:
            line = await reader.readline()
            if not line: return
            try:
                msg = json.loads(line.strip())
            except Exception:
                return
            if msg.get("type") != "register":
                return
            wid = msg.get("wid") or str(peer)
            # [F2] 认证
            if require_auth:
                token = msg.get("token", "")
                if not _check_token(secret, wid, token):
                    _warn(f"Worker 注册被拒绝 (token 无效): {wid} from {peer}")
                    writer.write(json.dumps({"type": "auth_failed"}).encode() + b"\n")
                    await writer.drain()
                    return
            agg.register(wid)
            last_seen[wid] = time.monotonic()
            print(f"[Master] Worker 注册: {wid} ({peer}) coroutines={msg.get('coroutines', '?')}")
            # [FIX7] 注册确认回执: Worker 侧等待该回执后再开始压测 (认证失败路径已回 auth_failed)
            writer.write(json.dumps({"type": "registered"}).encode() + b"\n")
            await writer.drain()

            while True:
                line = await reader.readline()
                if not line: break
                line = line.strip()
                if not line: continue
                try: m = json.loads(line)
                except Exception: continue
                m["wid"] = wid
                if m.get("type") == "heartbeat":
                    last_seen[wid] = time.monotonic()
                    continue
                agg.apply(m)
                if m.get("final"):
                    last_seen[wid] = time.monotonic()
        except Exception:
            pass
        finally:
            try: writer.close()
            except Exception: pass

    # [F2] 可选 TLS
    ssl_ctx = None
    if args.master_tls_cert and args.master_tls_key:
        try:
            ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_ctx.load_cert_chain(args.master_tls_cert, args.master_tls_key)
            if args.master_tls_ca:
                ssl_ctx.load_verify_locations(args.master_tls_ca)
                ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        except Exception as e:
            _error(f"Master TLS 初始化失败: {e}"); sys.exit(2)

    server = await asyncio.start_server(_handle, "0.0.0.0", args.master_port,
                                        limit=64 * 1024, ssl=ssl_ctx)
    print(f"[Master] 监听 0.0.0.0:{args.master_port} "
          f"auth={'HMAC' if require_auth else 'OFF'} tls={'on' if ssl_ctx else 'off'} (Ctrl+C 结束收集)...")
    start = time.monotonic(); last_print = start; last_req = 0
    grace = args.duration + 120 if args.duration > 0 else None
    exit_code = 0

    async def _heartbeat_watchdog():
        while not stop.is_set():
            await asyncio.sleep(5.0)
            now = time.monotonic()
            for w in list(last_seen):
                if now - last_seen[w] > heartbeat_timeout:
                    _warn(f"Worker 失联 (心跳超时 {heartbeat_timeout:.0f}s): {w}")
                    last_seen.pop(w, None)
                    agg.drop_worker(w)  # [FIX3] 同步剔除 latest 中非 final 快照

    hb_task = asyncio.create_task(_heartbeat_watchdog())
    try:
        while not stop.is_set():
            await asyncio.sleep(0.5)
            now = time.monotonic()
            if now - last_print >= args.interval:
                t = agg.totals()
                rps = (t["req"] - last_req) / max(now - last_print, 1e-6)
                print(f"[Master {int(now-start):4d}s] workers={len(agg.latest)} req={t['req']} ok={t['ok']} "
                      f"fail={t['fail']} rps={rps:.1f} p99={merged_percentile(t['hist'], t['count'], 99):.1f}ms", flush=True)
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
        hb_task.cancel()
        server.close()
        try: await server.wait_closed()
        except Exception: pass
        restore_signal_handlers(kind, old_int, old_term)

    elapsed = time.monotonic() - agg.start_time
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
        ssl_ctx = None
        if args.master_tls_cert and args.master_tls_key:
            ssl_ctx = ssl.create_default_context()
            if args.master_tls_ca:
                ssl_ctx.load_verify_locations(args.master_tls_ca)
            else:
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(args.master_host, args.master_port, ssl=ssl_ctx), timeout=10.0)
    except Exception as e:
        _error(f"无法连接 Master {args.master_host}:{args.master_port}: {e}")
        return

    print(f"[Worker] 已连接 Master, id={wid}")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    kind, old_int, old_term = setup_signal_handlers(stop_event, loop)

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

    async def _heartbeat_loop():
        while not stop_event.is_set():
            await asyncio.sleep(args.master_heartbeat / 2.0 if args.master_heartbeat > 0 else 5.0)
            try:
                writer.write(json.dumps({"type": "heartbeat", "wid": wid}).encode() + b"\n")
                await writer.drain()
            except Exception:
                return
    hb_task = asyncio.create_task(_heartbeat_loop())

    try:
        reg = {"type": "register", "wid": wid, "coroutines": args.workers}
        if args.master_secret:
            reg["token"] = _make_token(args.master_secret.encode(), wid)
        writer.write(json.dumps(reg).encode() + b"\n")
        await writer.drain()

        # [FIX7] 等待 Master 注册确认: 认证失败/对端不兼容时立即中止, 不浪费整场测试时长
        try:
            ack_line = await asyncio.wait_for(reader.readline(), timeout=15.0)
        except asyncio.TimeoutError:
            _error("等待 Master 注册确认超时 (15s), 中止 — 检查 --master-secret 与对端版本")
            return
        ack_type = ""
        if ack_line:
            try: ack_type = json.loads(ack_line.strip()).get("type", "")
            except Exception: ack_type = ""
        if ack_type != "registered":
            _error(f"Master 注册未通过 (type={ack_type or 'connection-closed'}), 中止")
            return

        stats, groups = await run_load(args, norm_targets, body, engine, has_tcp, stop_event, on_report=on_report)

        final = stats.snapshot(wid=wid, final=True)
        writer.write(json.dumps(final).encode() + b"\n")
        await writer.drain()
        print(f"[Worker] 压测完成, 最终统计已上报 Master (req={stats.requests})")
    finally:
        hb_task.cancel()
        drain_task.cancel()
        try: writer.close()
        except Exception: pass
        restore_signal_handlers(kind, old_int, old_term)

# ===========================================================================
# 参数校验
# ===========================================================================
def validate_args(args) -> None:
    positive = {
        "workers": args.workers, "timeout": args.timeout,
        "discovery_timeout": args.discovery_timeout,
        "keepalive_timeout": args.keepalive_timeout,
        "read_bufsize": args.read_bufsize,
        "max_conns_per_ep": args.max_conns_per_ep,
        "h2_conns_per_ep": args.h2_conns_per_ep,
        "arrival_queue": args.arrival_queue,
        "processes": args.processes,
        "master_port": args.master_port,
        "drain_timeout": args.drain_timeout,
        "runtime_telemetry_interval": args.runtime_telemetry_interval,
        "h3_conns": args.h3_conns,
        "rust_runtime_threads": args.rust_runtime_threads if args.rust_runtime_threads else 1,
    }
    for name, value in positive.items():
        if value <= 0:
            raise ValueError(f"--{name.replace('_', '-')} 必须大于 0")
    non_negative = {
        "duration": args.duration, "warmup": args.warmup,
        "rps": args.rps, "rps_burst": args.rps_burst,
        "ramp_up": args.ramp_up, "ep_cooldown": args.ep_cooldown,
        "tcp_churn": args.tcp_churn, "circuit_breaker": args.circuit_breaker,
        "ep_recovery": args.ep_recovery, "export_sample": args.export_sample,
        "master_heartbeat": args.master_heartbeat, "master_heartbeat_timeout": args.master_heartbeat_timeout,
    }
    for name, value in non_negative.items():
        if value is not None and value < 0:
            raise ValueError(f"--{name.replace('_', '-')} 不能小于 0")
    if args.export_sample < 1:
        raise ValueError("--export-sample 必须 >= 1")
    if args.default_scheme not in ("http", "https"):
        raise ValueError("--default-scheme 仅支持 http 或 https")
    if args.cpu_core is not None and args.cpu_core < 0:
        raise ValueError("--cpu-core 不能小于 0")
    # Universal engine contract: transport-specific TCP knobs remain accepted.
    # Only raw engine applies them at socket level; other engines report them as neutralized.
    if args.serve and args.master_host:
        raise ValueError("--serve 与 --master-host 不能同时使用")
    if args.serve and args.processes != 1:
        raise ValueError("远程 Master 不使用 --processes")
    if args.rate_mode == "arrival" and args.rps <= 0:
        raise ValueError("--rate-mode arrival 需要 --rps 大于 0")
    if args.arrival_drop_policy not in ("drop", "block", "error"):
        raise ValueError("--arrival-drop-policy 仅支持 drop|block|error")
    if args.log_format not in ("text", "json"):
        raise ValueError("--log-format 仅支持 text|json")
    if args.threshold:
        parsed = parse_thresholds(args.threshold)
        if not parsed or len(parsed) != len([p for p in args.threshold.split(';') if p.strip()]):
            raise ValueError("--threshold 格式无效")
        valid_keys = {"p50", "p95", "p99", "p999", "ttfb_p99", "error_rate", "rps"}
        for key, _, value in parsed:
            if key not in valid_keys and not re.fullmatch(r"status_[1-5]\d\d", key):
                raise ValueError(f"不支持的阈值指标: {key}")
            try:
                float(value[:-2] if value.endswith("ms") else value[:-1] if value.endswith(("s", "%")) else value)
            except ValueError as exc:
                raise ValueError(f"阈值数值无效: {value}") from exc


# ===========================================================================
# [F13] 生效配置输出
# ===========================================================================
def show_effective_config(args, engine):
    print("\n===== EFFECTIVE CONFIG (v" + VERSION + ") =====")
    for k in sorted(vars(args)):
        v = getattr(args, k)
        if k in ("master_secret",) and v:
            v = "***"
        print(f"  {k:28s} = {v}")
    print(f"  {'engine(resolved)':28s} = {engine}")
    print("=" * 40)

# ===========================================================================
# Main
# ===========================================================================
def _self_test():
    """Deterministic offline smoke tests for parser/metrics/Rust core build."""
    import tempfile, subprocess, py_compile, inspect
    tests = []
    try:
        py_compile.compile(__file__, doraise=True)
        tests.append(("python-compile", True))
    except Exception as e:
        _error(f"self-test python-compile failed: {e}")
        tests.append(("python-compile", False))
    # Build embedded Rust core only; do not touch the network.
    try:
        path = _ensure_rust_binary()
        r = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=20)
        tests.append(("rust-core", r.returncode == 0 and "stresscore" in (r.stdout + r.stderr)))
    except RuntimeError as e:
        # rustc is optional; absence is not a failure of the Python engines.
        if "rustc not found" in str(e):
            print("SELFTEST rust-core: SKIP (rustc unavailable in current environment)")
            tests.append(("rust-core", True))
        else:
            _error(f"self-test rust-core failed: {e}")
            tests.append(("rust-core", False))
    except Exception as e:
        _error(f"self-test rust-core failed: {e}")
        tests.append(("rust-core", False))
    # Static invariant checks for the embedded Rust core. These do not require rustc.
    source_checks = {
        "rust-half-open-single-flight": "probe_in_flight" in RUST_CORE_SRC and "State::HalfOpen" in RUST_CORE_SRC,
        "rust-arrival-error-stops": "Backpressure::Error" in RUST_CORE_SRC and "stop2.store(true" in RUST_CORE_SRC,
        "rust-fractional-duration": "parse::<f64>()" in RUST_CORE_SRC and "dur_secs" in RUST_CORE_SRC,
        "rust-http-no-body-status": "body_forbidden" in RUST_CORE_SRC and "status == 204" in RUST_CORE_SRC,
        "rust-global-connect-deadline": "let deadline = Instant::now() + timeout" in RUST_CORE_SRC,
        "yaml-explicit-cli-precedence": callable(globals().get("_explicit_cli_dests")) and "dest in explicit_dests" in inspect.getsource(_apply_yaml_config),
        "engine-explicit-fail-fast": "显式引擎禁止静默降级" in inspect.getsource(_preflight_engine),
        "rust-prefer-ips-universal": "--prefer-ips" in inspect.getsource(_run_rust_engine) and "return True" in inspect.getsource(_rust_scenario_supported),
        "rust-csv-universal": "return True" in inspect.getsource(_rust_scenario_supported),
        "scenario-ir-deterministic": ScenarioIR((), "GET", "/", hashlib.sha256(b"").hexdigest(), _headers_fingerprint([]), 1, 1, 1.0, 0.0, 1.0, "rr", "raw", 0.0, "worker", 0.0, 1, "drop", True, False, False, True, 1, 1).digest == ScenarioIR((), "GET", "/", hashlib.sha256(b"").hexdigest(), _headers_fingerprint([]), 1, 1, 1.0, 0.0, 1.0, "rr", "raw", 0.0, "worker", 0.0, 1, "drop", True, False, False, True, 1, 1).digest,
        "runtime-telemetry": RuntimeTelemetry(0.2).classification(Stats(), 65536) == "UNKNOWN",
        "html-self-contained": ("fastly.jsdelivr.net" not in inspect.getsource(generate_html_report) and "new Chart(" not in inspect.getsource(generate_html_report)),
        "keepalive-cli-bridge": "--body-mode" in inspect.getsource(_run_rust_engine) and "no_keepalive" in inspect.getsource(make_aiohttp_connector),
        "no-startup-burst-default": "self._tokens = float(self.capacity if burst is not None else 0.0)" in inspect.getsource(Scheduler.__init__),
        "rust-native-protocol-matrix": all(x in _RUST_NATIVE_SRC for x in ("Proto { H1, H2, H3, WS }", "run_hyper", "run_h3", "run_ws")),
        "rust-single-core-1m-target": "single_core_target_rps" in _RUST_NATIVE_SRC,
        "rust-real-worker-sharding": 'run_hyper_worker' in _RUST_NATIVE_SRC and 'FuturesUnordered' in _RUST_NATIVE_SRC and 'for id in 0..c.workers.max(1)' in _RUST_NATIVE_SRC,
        "rust-local-scheduler-counter": 'let mut scheduled=0u64' in _RUST_NATIVE_SRC and 'saturating_sub(scheduled)' in _RUST_NATIVE_SRC,
        "rust-adaptive-batch-cap": _RUST_NATIVE_SRC.count('batch_cap=if base_per_rps>0.0') >= 2 and '32768' in _RUST_NATIVE_SRC,
        "rust-single-core-runtime": 'new_current_thread()' in _RUST_NATIVE_SRC and 'benchmark_single_core' in _RUST_NATIVE_SRC,
        "rust-cpu-affinity": 'sched_setaffinity' in _RUST_NATIVE_SRC and '--cpu-core' in _RUST_NATIVE_SRC,
        "rust-h3-multi-connection": 'let n=c.h3_conns.max(1)' in _RUST_NATIVE_SRC and 'h3_connection' in _RUST_NATIVE_SRC,
        "rust-h3-byte-accounting": 'bytes_in:bin' in _RUST_NATIVE_SRC and 'chunk.remaining()' in _RUST_NATIVE_SRC,
        "rust-body-streaming": 'consume_hyper_body' in _RUST_NATIVE_SRC and 'body.frame().await' in _RUST_NATIVE_SRC,
        "rust-percentiles": 'p999_us' in _RUST_NATIVE_SRC and 'Histogram' in _RUST_NATIVE_SRC,
        "rust-cargo-preflight": '_cargo_find()' in inspect.getsource(_preflight_engine) and '_cargo_find()' in inspect.getsource(_resolve_engine),
        "rust-h3-concurrent-streams": 'SendRequest<h3_quinn::Connection,Bytes>' in _RUST_NATIVE_SRC and 'send.clone()' in _RUST_NATIVE_SRC and 'JoinSet' in _RUST_NATIVE_SRC,
        "rust-h2-https": 'HttpsConnectorBuilder' in _RUST_NATIVE_SRC and 'enable_http2' in _RUST_NATIVE_SRC,
        "rust-ws-persistent": 'connect_async_with_config' in _RUST_NATIVE_SRC and 'ws_handshakes' in _RUST_NATIVE_SRC and 'WebSocketConfig::default()' in _RUST_NATIVE_SRC,
        "rust-direct-rustls-dependency": 'rustls = "0.23"' in _RUST_NATIVE_CARGO_TOML,
        "security-profile-defensive-only": 'defensive-only' in inspect.getsource(_security_profile_summary),
        "rust-no-invalid-ws-config-field": "max_send_queue" not in _RUST_NATIVE_SRC,
        "rust-uri-format-valid": "format!('/" not in _RUST_NATIVE_SRC,
        "local-start-barrier": '_start_at' in inspect.getsource(master_loop) and 'deadline - time.monotonic()' in inspect.getsource(worker_process_main),
        "cpu-one-core-metric": 'max_cpu_one_core_pct' in inspect.getsource(RuntimeTelemetry.run) and 'max_cpu_one_core_pct' in inspect.getsource(RuntimeTelemetry.classification),
        "rust-header-bridge": 'for h in getattr(args,"header",[])' in inspect.getsource(_run_rust_engine) and 'parse_headers' in _RUST_NATIVE_SRC,
        "rust-ws-echo-mode": 'ws_echo_success' in _RUST_NATIVE_SRC and 'WsMode::Echo' in _RUST_NATIVE_SRC,
        "deterministic-default": 'action="store_true", default=False' in inspect.getsource(main) and '--randomize' in inspect.getsource(main),
        "rust-bytes-out": 'bytes_out=out.attempts.saturating_mul' in _RUST_NATIVE_SRC,
        "single-core-error-contained": 'except (ValueError, RuntimeError) as exc' in inspect.getsource(_run_single_process),
    }
    source_checks.update({
        "rust-h3-status-variable-fixed": "let status_u=resp.status().as_u16();" in _RUST_NATIVE_SRC,
        "rust-global-inflight-sharding": 'worker_flight=' in _RUST_NATIVE_SRC and 'c.max_in_flight' in _RUST_NATIVE_SRC,
        "universal-max-in-flight-cli": 'p.add_argument("--max-in-flight"' in inspect.getsource(main),
        "universal-tcp-options-no-hard-gate": 'TCP 干扰参数与 --engine' not in inspect.getsource(_preflight_engine),
        "universal-rate-params-bridge": 'rps-burst' in inspect.getsource(_run_rust_engine) and 'rate_mode' in inspect.getsource(_run_rust_engine) and 'ramp-up' in inspect.getsource(_run_rust_engine),
        "rust-no-undefined-h3-status": 'status:status,' not in _RUST_NATIVE_SRC,
        "rust-version-37": '"version":"37.0.0"' in _RUST_NATIVE_SRC,
        "rust-no-unbound-per-rps": re.search(r'\bper_rps\b', _RUST_NATIVE_SRC) is None,
    })
    tests.extend(source_checks.items())
    failed = [name for name, ok in tests if not ok]
    for name, ok in tests:
        print(f"SELFTEST {name}: {'PASS' if ok else 'FAIL'}")
    return 1 if failed else 0

def _explicit_cli_dests(parser, argv):
    """Return argparse dest names that were explicitly present in argv."""
    option_to_dest = {}
    for action in parser._actions:
        for opt in action.option_strings:
            option_to_dest[opt] = action.dest
    explicit = set()
    for token in argv:
        if not token.startswith("-"):
            continue
        name = token.split("=", 1)[0]
        dest = option_to_dest.get(name)
        if dest:
            explicit.add(dest)
    return explicit

def _apply_yaml_config(args, cfg, explicit_dests):
    """Apply YAML only to fields not explicitly supplied on the CLI."""
    yaml_to_dest = {
        "headers": "header",
    }
    yaml_keys = (
        "targets", "path", "method", "body", "body_json", "headers", "workers",
        "duration", "warmup", "timeout", "mode", "engine", "rps", "rate_mode",
        "rps_burst", "arrival_queue", "arrival_drop_policy", "ramp_up", "threshold",
        "data_file", "format", "randomize", "insecure", "ignore_status", "skip_body",
        "max_conns_per_ep", "h2_conns_per_ep", "ep_breaker", "ep_recovery",
        "report_html", "export", "export_file", "export_sample", "no_discovery",
        "discovery_timeout", "uvloop", "log_format", "prefer_ips", "prefer_h3",
        "prefer_rust", "keepalive_timeout", "read_bufsize", "drain_timeout",
        "default_scheme", "interval", "circuit_breaker", "no_keepalive", "benchmark", "security_layer", "start_delay",
        "rust_runtime_threads", "cpu_core", "h3_conns", "ws_mode",
    )
    for key in yaml_keys:
        if key not in cfg:
            continue
        dest = yaml_to_dest.get(key, key)
        if dest in explicit_dests:
            continue
        # body/body_json are mutually exclusive at the effective-config layer.
        if key == "body" and "body_json" in explicit_dests:
            continue
        if key == "body_json" and "body" in explicit_dests:
            continue
        value = cfg[key]
        if key == "headers":
            if isinstance(value, dict):
                args.header = [f"{k}:{v}" for k, v in value.items()]
            elif isinstance(value, list):
                args.header = [str(x) for x in value]
            else:
                raise ValueError("YAML headers 必须是 mapping 或 list")
        elif key == "targets":
            args.targets = ",".join(map(str, value)) if isinstance(value, list) else str(value)
        else:
            setattr(args, dest, value)
    # YAML itself must not be allowed to bypass the explicit --body / --body-json contract.
    if "body" in cfg and "body_json" in cfg and cfg.get("body") is not None and cfg.get("body_json") is not None:
        if "body" not in explicit_dests and "body_json" not in explicit_dests:
            raise ValueError("YAML 中 body 与 body_json 不能同时设置")

def _security_profile_summary(layer: str) -> dict:
    profiles = {
        "L0": "physical asset controls / secure boot / tamper evidence",
        "L1": "RF/interface health and inventory validation",
        "L2": "NAC / DHCP snooping / ARP inspection validation",
        "L3": "routing resilience / state-capacity validation",
        "L4": "transport timeout / retransmission / connection-capacity validation",
        "L5": "session integrity / TLS policy / token lifecycle validation",
        "L6": "serialization / parser-boundary / content-integrity validation",
        "L7": "application capacity / rate-limit / WAF-policy validation",
        "L8": "CI/CD provenance / SBOM / dependency-integrity validation",
        "L9": "identity / phishing-resistance / privileged-access governance validation",
        "all": "full defensive validation taxonomy L0-L9",
    }
    return {"layer": layer, "profile": profiles[layer], "mode": "defensive-only"}

def main():
    p = argparse.ArgumentParser(
        description=f"Termux HTTP Stress Test v{VERSION} — Hardened Native Data Plane\n"
                    "Single/Multi-Process/Distributed + HTTP/1.1/2/3 + WS")

    p.add_argument("--version", action="version", version=f"stress_test v{VERSION}")
    p.add_argument("--runtime-telemetry-interval", type=float, default=1.0, help="客户端资源/调度延迟采样间隔秒")
    p.add_argument("--benchmark", choices=["off", "single-core"], default="off", help="benchmark mode; single-core uses native Rust current-thread runtime; target only")
    p.add_argument("--security-layer", choices=["L0","L1","L2","L3","L4","L5","L6","L7","L8","L9","all"], default="all", help="defensive validation label only; no exploit, RF jamming, credential theft, or evasion actions")
    p.add_argument("--start-delay", type=float, default=0.0, help="local multi-process synchronized start delay in seconds")
    p.add_argument("--self-test", action="store_true", help="运行离线自检，不连接目标")
    p.add_argument("--targets", help="逗号分隔目标列表")
    p.add_argument("--file", help="目标文件, 每行一个")
    p.add_argument("--config", help="YAML 场景配置文件")
    p.add_argument("--path", default="/", help="默认路径")
    p.add_argument("--default-scheme", default="http")
    p.add_argument("--prefer-ips", help="手动指定后端 IP")
    p.add_argument("--prefer-h3", action="store_true", help="auto 模式优先 h3")
    p.add_argument("--prefer-rust", action="store_true", help="auto 模式在场景完全可映射时优先 Rust H1")

    p.add_argument("--method", default="GET")
    p.add_argument("--body", default=None)
    p.add_argument("--body-json", default=None)
    p.add_argument("--header", action="append", default=[])
    p.add_argument("--data-file", help="CSV 数据文件路径")
    p.add_argument("--format", help="URL 模板, 如 /api?user_id=${id}")

    # 分布式
    p.add_argument("--processes", type=int, default=1, help="本地多进程 Worker 数量 (RPS 自动均分)")
    p.add_argument("--serve", action="store_true", help="作为远程 Master 启动")
    p.add_argument("--master-port", type=int, default=8765, help="远程 Master 监听端口")
    p.add_argument("--master-host", type=str, default=None, help="连接到远程 Master (本进程作为 Worker)")
    # [F2] 认证 + TLS
    p.add_argument("--master-secret", type=str, default=None,
                   help="Master/Worker 共享 HMAC 密钥 (启用认证; 建议 >=32 字符随机)")
    p.add_argument("--master-tls-cert", type=str, default=None, help="Master TLS 证书 (PEM)")
    p.add_argument("--master-tls-key", type=str, default=None, help="Master TLS 私钥 (PEM)")
    p.add_argument("--master-tls-ca", type=str, default=None, help="CA 证书 (mTLS 验证)")
    p.add_argument("--master-heartbeat", type=float, default=5.0, help="Worker 心跳间隔秒")
    p.add_argument("--master-heartbeat-timeout", type=float, default=15.0, help="Master 判失联阈值秒")

    p.add_argument("-w", "--workers", type=int, default=50, help="每进程/Worker 协程数")
    p.add_argument("-d", "--duration", type=float, default=30.0)
    p.add_argument("--warmup", type=float, default=0, help="预热秒数(不计统计)")
    p.add_argument("--timeout", type=float, default=10.0)
    p.add_argument("--mode", choices=["rr", "random"], default="rr")
    p.add_argument("--engine", choices=["auto", "rust", "aiohttp", "raw", "h2", "h3", "ws"], default="auto",
                   help="rust = embedded Rust data-plane (recommended for max throughput/accuracy)")
    p.add_argument("--rust-protocol", choices=["auto", "h1", "h2", "h3", "ws"], default="auto", help="Rust native protocol backend")
    p.add_argument("--rust-burst-window-ms", type=int, default=10, help="Rust pacing batch window 1..100ms")
    p.add_argument("--rust-runtime-threads", type=int, default=0, help="native Rust runtime threads; 0=auto; single-core forces 1")
    p.add_argument("--cpu-core", type=int, default=None, help="pin native Rust process to one CPU core when supported")
    p.add_argument("--h3-conns", type=int, default=1, help="native Rust H3 QUIC connections per endpoint")
    p.add_argument("--max-in-flight", dest="max_in_flight", type=int, default=16384, help="global maximum in-flight operations; sharded across workers")
    p.add_argument("--ws-mode", choices=["send", "echo"], default="send", help="native Rust WS send-only or echo RTT mode")
    p.add_argument("--max-conns-per-ep", type=int, default=64)
    p.add_argument("--h2-conns-per-ep", type=int, default=4)
    p.add_argument("--ep-cooldown", type=float, default=2.0)
    # [F9] 熔断
    p.add_argument("--ep-breaker", type=int, default=5, help="端点连续失败 N 次熔断 OPEN")
    p.add_argument("--ep-recovery", type=float, default=30.0, help="熔断 OPEN 后恢复探测间隔秒")

    p.add_argument("--rps", type=float, default=0)
    p.add_argument("--rate-mode", choices=["worker", "arrival"], default="worker")
    p.add_argument("--rps-burst", type=float, default=None)
    p.add_argument("--arrival-queue", type=int, default=4096)
    # [F3] 背压策略
    p.add_argument("--arrival-drop-policy", choices=["drop", "block", "error"], default="drop",
                   help="arrival 队列满时策略: drop(丢弃计数)|block(阻塞)|error(停止)")
    p.add_argument("--ramp-up", type=float, default=0)

    p.add_argument("--threshold", type=str, default=None,
                   help="断言, 如 'p99<500ms;error_rate<1%%;status_200>=99%%;ttfb_p99<200ms'")
    p.add_argument("--circuit-breaker", type=int, default=0, help="连续失败达到此值停止; 0 禁用")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--report-html", type=str, default=None)

    p.add_argument("--randomize", action="store_true", default=False)
    p.add_argument("--no-randomize", dest="randomize", action="store_false")
    p.add_argument("--insecure", action="store_true")
    p.add_argument("--uvloop", action="store_true")
    p.add_argument("--ignore-status", action="store_true")
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--skip-body", action="store_true")
    p.add_argument("--body-mode", choices=["count","discard"], default="count", help="universal response-body semantics; discard avoids body-byte accounting")
    p.add_argument("--no-keepalive", action="store_true", help="每次请求均关闭连接，不复用 Keep-Alive")
    p.add_argument("--keepalive-timeout", type=float, default=60.0)
    p.add_argument("--read-bufsize", type=int, default=65536)
    p.add_argument("--drain-timeout", type=float, default=5.0, help="优雅退出等待在途请求秒数")

    # TCP 干扰
    p.add_argument("--tcp-sndbuf", type=int, default=None)
    p.add_argument("--tcp-rcvbuf", type=int, default=None)
    p.add_argument("--tcp-window-clamp", type=int, default=None)
    p.add_argument("--tcp-nagle", action="store_true")
    p.add_argument("--tcp-linger-rst", action="store_true")
    p.add_argument("--tcp-churn", type=int, default=0)
    p.add_argument("--tcp-small-segs", action="store_true")

    # [B8] 导出
    p.add_argument("--export", choices=["json", "csv"], default=None)
    p.add_argument("--export-file", default="stress_results")
    p.add_argument("--export-sample", type=int, default=1, help="每 N 请求写一行详情")

    p.add_argument("--no-discovery", action="store_true")
    p.add_argument("--discovery-timeout", type=float, default=5.0)
    p.add_argument("--i-have-authorization", action="store_true",
                   help="确认已获授权")
    # [F12/F13]
    p.add_argument("--log-format", choices=["text", "json"], default="text")
    p.add_argument("--show-effective-config", action="store_true", help="打印生效配置后退出")

    args = p.parse_args()

    if args.self_test:
        sys.exit(_self_test())

    if args.config:
        cfg = load_yaml_config(args.config) or {}
        explicit_dests = _explicit_cli_dests(p, sys.argv[1:])
        try:
            _apply_yaml_config(args, cfg, explicit_dests)
        except ValueError as exc:
            _error(f"YAML 配置无效: {exc}")
            sys.exit(2)

    if not args.i_have_authorization:
        print(AUTHORIZATION_BANNER)
        print("[ERROR] Use --i-have-authorization to confirm you have permission to test.")
        sys.exit(1)
    try:
        validate_args(args)
        if args.benchmark == "single-core" and args.processes != 1:
            raise ValueError("--benchmark single-core 目前要求 --processes 1")
        if args.benchmark == "single-core" and args.engine not in ("auto", "rust"):
            raise ValueError("--benchmark single-core 需要 --engine rust 或 --engine auto")
        if args.start_delay < 0:
            raise ValueError("--start-delay 不能小于 0")
    except ValueError as exc:
        _error(f"参数无效: {exc}")
        sys.exit(2)

    # [F12]
    global LOG_FORMAT
    LOG_FORMAT = args.log_format

    if args.uvloop:
        if HAS_UVLOOP:
            try: asyncio.set_event_loop_policy(uvloop.EventLoopPolicy()); _info("uvloop 已启用")
            except Exception as e: _warn(f"uvloop 初始化失败 ({e}), 回退至默认 asyncio")
        else: _warn("uvloop 不可用")

    body = None
    if args.body is not None:
        body = args.body.encode()
    elif args.body_json is not None:
        try:
            body = json_dumps(json.loads(args.body_json))
            args.body = body.decode("utf-8")
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            _error(f"--body-json 无效: {exc}"); sys.exit(2)

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
        except FileNotFoundError: _error(f"文件不存在: {args.file}"); sys.exit(1)
    elif args.targets: raw_targets = [t.strip() for t in args.targets.split(",") if t.strip()]
    elif os.getenv("TARGETS"):
        raw_targets = [t.strip() for t in os.getenv("TARGETS").split(",") if t.strip()]
        _warn("检测到环境变量 TARGETS 作为目标来源")
    elif not args.serve:
        _error("未配置目标"); sys.exit(1)

    norm_targets = []
    for entry in raw_targets:
        entry = entry.strip().strip('"').strip("'")
        if not entry or entry.startswith("#"): continue
        if "://" not in entry: entry = f"{args.default_scheme}://{entry}"
        norm_targets.append(entry)
    if not norm_targets and not args.serve:
        _error("无有效目标"); sys.exit(1)

    engine = args.engine
    has_tcp = any([args.tcp_sndbuf is not None, args.tcp_rcvbuf is not None, args.tcp_window_clamp is not None,
                   args.tcp_nagle, args.tcp_linger_rst, args.tcp_churn > 0, args.tcp_small_segs])
    try:
        _preflight_engine(args, has_tcp)
    except ValueError as exc:
        _error(f"引擎配置无效: {exc}")
        sys.exit(2)

    # [F11] fd 预检
    fd_limit = raise_fd_limit()
    need_fd = args.workers * max(1, args.max_conns_per_ep // 4) + 64
    if fd_limit < need_fd:
        _warn(f"fd 上限 {fd_limit} 可能不足 (估算需 ~{need_fd}); 建议 ulimit -n 65536")

    if args.show_effective_config:
        show_effective_config(args, engine)
        return

    print("=" * 70 + f"\nTERMUX HTTP STRESS TEST v{VERSION} (Industrial Edition)\n" + "=" * 70)
    print(f"Engine           : {engine}")
    print(f"Rust protocol    : {getattr(args, 'rust_protocol', 'auto')}")
    print(f"Scenario IR      : pending (final digest emitted after discovery)")
    print(f"Effective target semantics: {'exact-rust-bridge' if engine == 'rust' else 'python-engine'}")
    if args.serve:
        print(f"Mode             : Remote Master (port {args.master_port})")
    elif args.master_host:
        print(f"Mode             : Remote Worker -> {args.master_host}:{args.master_port}")
    elif args.processes > 1:
        print(f"Mode             : Local Multi-Process ({args.processes} procs)")
    else:
        print(f"Mode             : Single Process")
    print(f"Event loop       : {'uvloop' if (args.uvloop and HAS_UVLOOP) else 'asyncio'}")
    print(f"fd limit         : {fd_limit}")
    print(f"Targets          : {len(norm_targets)}")
    print(f"Workers per proc : {args.workers}")
    print(f"Duration         : {args.duration}s")
    print(f"Warmup           : {args.warmup}s" if args.warmup > 0 else "Warmup           : off")
    print(f"Timeout          : {args.timeout}s")
    print(f"Method           : {args.method}")
    print(f"Mode             : {args.mode}")
    print(f"Randomize        : {args.randomize}")
    print(f"RPS limit        : {args.rps if args.rps > 0 else 'unlimited'}")
    print(f"Benchmark        : {args.benchmark}")
    sec=_security_profile_summary(args.security_layer)
    print(f"Security profile : {sec['layer']} ({sec['mode']})")
    print(f"Rate mode        : {args.rate_mode}" + (f" drop_policy={args.arrival_drop_policy}" if args.rate_mode == "arrival" else ""))
    print(f"Data file        : {args.data_file or 'off'}" + (f" sample=1/{args.export_sample}" if args.export else ""))
    print(f"Ramp-up          : {args.ramp_up}s" if args.ramp_up > 0 else "Ramp-up          : off")
    print(f"Ignore status    : {args.ignore_status}")
    print(f"Skip body        : {args.skip_body}")
    print(f"Keep-Alive       : {not args.no_keepalive}")
    print(f"TLS insecure     : {args.insecure}")
    print(f"EP breaker       : {args.ep_breaker} fails -> OPEN {args.ep_recovery:.0f}s")
    print(f"Circuit breaker  : {args.circuit_breaker if args.circuit_breaker > 0 else 'off'}")
    if args.threshold: print(f"Thresholds       : {args.threshold}")
    if args.serve or args.master_host:
        print(f"Master auth      : {'HMAC' if args.master_secret else 'OFF'} tls={'on' if args.master_tls_cert else 'off'}")
    if has_tcp:
        print("-" * 70 + "\nTCP 干扰:")
        if args.tcp_sndbuf is not None: print(f"  SO_SNDBUF        : {args.tcp_sndbuf}")
        if args.tcp_rcvbuf is not None: print(f"  SO_RCVBUF        : {args.tcp_rcvbuf}")
        if args.tcp_window_clamp is not None: print(f"  TCP_WINDOW_CLAMP : {args.tcp_window_clamp}")
        print(f"  Nagle (禁NODELAY): {args.tcp_nagle}")
        print(f"  SO_LINGER RST    : {args.tcp_linger_rst}")
        print(f"  Churn interval   : {args.tcp_churn if args.tcp_churn > 0 else 'off'}")
        print(f"  Small segments   : {args.tcp_small_segs}")
    print("Rust 1M RPS      : single-core benchmark target (not a universal guarantee)")
    print("=" * 70)

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
