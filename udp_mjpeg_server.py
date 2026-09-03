#!/usr/bin/env python3
"""
udp_mjpeg_server.py

Receives chunked RGB565 frames from the ESP32 over UDP, reassembles them,
encodes JPEG, and serves an MJPEG stream that any browser on the LAN can open.

    python udp_mjpeg_server.py

By default this also:
  - detects this machine's own LAN IP and prints it (no manual lookup needed)
  - registers an mDNS service ("_ov7670._udp.local.") via the `zeroconf`
    package, so esp32_mdns_stream.ino can find this machine automatically --
    this does NOT require Bonjour/mDNSResponder to be installed, `zeroconf`
    implements the protocol itself, which is what makes it work on a plain
    Windows PC
  - opens your default browser to the live stream

Flip these off with --no-mdns / --no-browser if you're running the broadcast
firmware (esp32_broadcast_stream.ino) instead, which needs neither.

Packet layout (little-endian, must match the firmware):
    magic 'OV' (2) | frame_id u16 | chunk_id u16 | total_chunks u16 |
    width u16 | height u16 | payload

Incomplete frames are discarded, never waited for. Dropping a frame is always
cheaper than stalling a live video pipeline.
"""

import argparse
import socket
import struct
import subprocess
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import cv2
import numpy as np

HEADER_FMT = "<2sHHHHH"
HEADER_SIZE = struct.calcsize(HEADER_FMT)  # 12
MAGIC = b"OV"
BOUNDARY = "frameboundary"

MDNS_SERVICE_TYPE = "_ov7670._udp.local."
MDNS_INSTANCE_NAME = "ov7670server"


def get_local_ip():
    """Best-effort LAN IP without needing any traffic to actually go out.

    Opens a UDP socket and "connects" it to a public address purely so the
    OS picks a source interface/IP for that route -- no packet is sent for
    connect() on SOCK_DGRAM. Works the same way on macOS, Linux and Windows.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def register_mdns(local_ip, udp_port):
    """Advertise this machine as the ov7670 UDP receiver on the LAN (zeroconf).

    Used on Windows/Linux; macOS goes through register_mdns_darwin instead.
    Returns a zero-arg cleanup callable to unregister on shutdown, or None if
    the `zeroconf` package isn't installed.
    """
    try:
        from zeroconf import Zeroconf, ServiceInfo
    except ImportError:
        print("zeroconf package not installed -- mDNS advertising disabled.")
        print("Install it with: pip install zeroconf")
        return None

    hostname = socket.gethostname().split(".")[0]
    info = ServiceInfo(
        MDNS_SERVICE_TYPE,
        f"{MDNS_INSTANCE_NAME}.{MDNS_SERVICE_TYPE}",
        addresses=[socket.inet_aton(local_ip)],
        port=udp_port,
        properties={},
        server=f"{hostname}.local.",
    )
    zc = Zeroconf()
    zc.register_service(info)
    print(f"mDNS: advertising as {MDNS_SERVICE_TYPE} on {local_ip}:{udp_port} "
          f"(host {hostname}.local.)")

    def cleanup():
        zc.unregister_service(info)
        zc.close()

    return cleanup


def register_mdns_darwin(udp_port):
    """Advertise via the macOS system responder (`dns-sd -R`) instead of zeroconf.

    On macOS, mDNSResponder already owns UDP 5353, so python-zeroconf's second
    responder on the same port never receives inbound multicast queries from
    other hosts -- the ESP32's `MDNS.queryService` gets zero results even though
    this machine's own `dns-sd -B` sees the (self-announced, cached) service.
    Registering through `dns-sd -R` puts the record in mDNSResponder itself,
    which every device on the LAN already queries.

    Returns a zero-arg cleanup callable, or None if `dns-sd` is unavailable.
    """
    # "_ov7670._udp.local." -> "_ov7670._udp"
    service_type = MDNS_SERVICE_TYPE.split(".local")[0]
    try:
        proc = subprocess.Popen(
            ["dns-sd", "-R", MDNS_INSTANCE_NAME, service_type, "local", str(udp_port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        print("dns-sd not found -- mDNS advertising disabled.")
        return None

    time.sleep(0.3)  # give it a moment to fail loudly if the args are wrong
    if proc.poll() is not None:
        print("dns-sd -R exited immediately -- mDNS advertising disabled.")
        return None

    print(f"mDNS: advertising {service_type} via dns-sd on :{udp_port}")

    def cleanup():
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()

    return cleanup


class FrameStore:
    """Holds the most recent encoded JPEG and wakes up waiting HTTP clients."""

    def __init__(self):
        self._cond = threading.Condition()
        self._jpeg = None
        self._seq = 0

    def publish(self, jpeg_bytes):
        with self._cond:
            self._jpeg = jpeg_bytes
            self._seq += 1
            self._cond.notify_all()

    def wait_for_next(self, last_seq, timeout=5.0):
        with self._cond:
            if self._seq == last_seq:
                self._cond.wait(timeout)
            return self._jpeg, self._seq

    def latest(self):
        with self._cond:
            return self._jpeg


class Receiver(threading.Thread):
    daemon = True

    def __init__(self, store, port, chunk, quality, swap_bytes, flip, stale=0.5):
        super().__init__(name="udp-receiver")
        self.store = store
        self.port = port
        self.chunk = chunk
        self.quality = quality
        self.swap_bytes = swap_bytes
        self.flip = flip
        self.stale = stale
        self.pending = {}  # frame_id -> dict
        self.completed = 0
        self.dropped = 0

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # A big receive buffer absorbs the burst of ~28 packets per frame.
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        except OSError:
            pass
        sock.bind(("0.0.0.0", self.port))
        print(f"Listening for UDP frames on 0.0.0.0:{self.port}")

        last_report = time.time()
        last_completed = last_dropped = 0

        while True:
            packet, _ = sock.recvfrom(65535)
            if len(packet) <= HEADER_SIZE:
                continue

            magic, frame_id, chunk_id, total, w, h = struct.unpack_from(
                HEADER_FMT, packet, 0
            )
            if magic != MAGIC or total == 0:
                continue

            entry = self.pending.get(frame_id)
            if entry is None or entry["total"] != total:
                entry = {
                    "buf": bytearray(w * h * 2),
                    "seen": set(),
                    "total": total,
                    "w": w,
                    "h": h,
                    "t": time.time(),
                }
                self.pending[frame_id] = entry

            if chunk_id not in entry["seen"]:
                payload = packet[HEADER_SIZE:]
                offset = chunk_id * self.chunk
                end = offset + len(payload)
                if end <= len(entry["buf"]):
                    entry["buf"][offset:end] = payload
                    entry["seen"].add(chunk_id)

            if len(entry["seen"]) == total:
                del self.pending[frame_id]
                self._emit(entry)
                self.completed += 1

            self._evict()

            now = time.time()
            if now - last_report >= 2.0:
                fps = (self.completed - last_completed) / (now - last_report)
                lost = self.dropped - last_dropped
                print(f"{fps:5.1f} fps delivered | {lost} incomplete frames dropped")
                last_report, last_completed, last_dropped = (
                    now,
                    self.completed,
                    self.dropped,
                )

    def _evict(self):
        cutoff = time.time() - self.stale
        for fid in [k for k, v in self.pending.items() if v["t"] < cutoff]:
            del self.pending[fid]
            self.dropped += 1

    def _emit(self, entry):
        w, h = entry["w"], entry["h"]
        arr = np.frombuffer(bytes(entry["buf"]), dtype=np.uint8).reshape(h, w, 2)
        if self.swap_bytes:
            arr = arr[:, :, ::-1].copy()
        bgr = cv2.cvtColor(arr, cv2.COLOR_BGR5652BGR)
        if self.flip:
            bgr = cv2.flip(bgr, -1)
        ok, jpeg = cv2.imencode(
            ".jpg", bgr, [int(cv2.IMWRITE_JPEG_QUALITY), self.quality]
        )
        if ok:
            self.store.publish(jpeg.tobytes())


PAGE = """<!doctype html>
<meta charset="utf-8">
<title>OV7670 live</title>
<style>
  body { margin:0; background:#111; color:#ddd; font:400 14px/1.6 -apple-system,
         BlinkMacSystemFont, sans-serif; display:flex; flex-direction:column;
         align-items:center; justify-content:center; min-height:100vh; gap:12px }
  img  { image-rendering:pixelated; width:min(90vw, 640px); border-radius:6px }
</style>
<img src="/stream" alt="Live camera stream">
<p>OV7670 over UDP</p>
"""


def make_handler(store):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_GET(self):
            if self.path == "/":
                body = PAGE.encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            elif self.path == "/snapshot":
                jpeg = store.latest()
                if jpeg is None:
                    self.send_error(503, "No frame received yet")
                    return
                self.send_response(200)
                self.send_header("Content-Type", "image/jpeg")
                self.send_header("Content-Length", str(len(jpeg)))
                self.end_headers()
                self.wfile.write(jpeg)

            elif self.path == "/stream":
                self.send_response(200)
                self.send_header("Age", "0")
                self.send_header("Cache-Control", "no-cache, private")
                self.send_header(
                    "Content-Type",
                    f"multipart/x-mixed-replace; boundary={BOUNDARY}",
                )
                self.end_headers()
                seq = 0
                try:
                    while True:
                        jpeg, seq = store.wait_for_next(seq)
                        if jpeg is None:
                            continue
                        self.wfile.write(f"--{BOUNDARY}\r\n".encode())
                        self.wfile.write(b"Content-Type: image/jpeg\r\n")
                        self.wfile.write(
                            f"Content-Length: {len(jpeg)}\r\n\r\n".encode()
                        )
                        self.wfile.write(jpeg)
                        self.wfile.write(b"\r\n")
                except (BrokenPipeError, ConnectionResetError):
                    pass
            else:
                self.send_error(404)

    return Handler


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--udp-port", type=int, default=5005)
    p.add_argument("--http-port", type=int, default=8080)
    p.add_argument("--chunk", type=int, default=1400, help="must match firmware")
    p.add_argument("--quality", type=int, default=80, help="JPEG quality 1-100")
    p.add_argument("--swap-bytes", action="store_true",
                   help="use if colors look wrong (byte-order mismatch)")
    p.add_argument("--flip", action="store_true", help="rotate the image 180 degrees")
    p.add_argument("--no-mdns", action="store_true",
                   help="skip mDNS self-registration (use with the broadcast firmware)")
    p.add_argument("--no-browser", action="store_true",
                   help="don't auto-open the stream in a browser on startup")
    args = p.parse_args()

    local_ip = get_local_ip()

    mdns_cleanup = None
    if not args.no_mdns:
        if sys.platform == "darwin":
            # zeroconf can't answer queries from other hosts on macOS (see
            # register_mdns_darwin) -- go through the system responder instead.
            mdns_cleanup = register_mdns_darwin(args.udp_port)
        else:
            mdns_cleanup = register_mdns(local_ip, args.udp_port)

    store = FrameStore()
    Receiver(store, args.udp_port, args.chunk, args.quality,
             args.swap_bytes, args.flip).start()

    server = ThreadingHTTPServer(("0.0.0.0", args.http_port), make_handler(store))
    # The socket is already bound and listening at this point (that happens
    # in ThreadingHTTPServer.__init__), so it's safe to open the browser as
    # soon as serve_forever starts on its own thread below.
    threading.Thread(target=server.serve_forever, daemon=True).start()

    url = f"http://{local_ip}:{args.http_port}/"
    print("=" * 60)
    print(f"  Camera stream ready:  {url}")
    print(f"  (also reachable at    http://localhost:{args.http_port}/ on this PC)")
    print("=" * 60)

    if not args.no_browser:
        webbrowser.open(url)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        if mdns_cleanup:
            mdns_cleanup()


if __name__ == "__main__":
    main()
