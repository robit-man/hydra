#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cross-platform HTTPS dev server (Windows/macOS/Linux)

🔧 What this fixes:
- Prompts read from the REAL console (CONIN$/dev/tty). Hitting Enter in cmd/PowerShell
  reliably returns a blank line → defaults are applied; the script does NOT exit.
- Venv bootstrap is robust and Windows-safe (short path under %LOCALAPPDATA% if needed).
- Uses `python -m pip` everywhere (no brittle pip.exe paths).
- All original features: self-signed cert, LE/Step/GCP CA modes, Node TLS patching,
  Python HTTPS fallback, cleanup, banner, etc.

Run:
  python server.py
"""

import os
import sys
import subprocess
import json
import shutil
import threading
import itertools
import time
import socket
import ssl
import urllib.request
import signal
import atexit
import argparse
import hashlib
import platform
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, SimpleHTTPRequestHandler

# ─── OS Flags ────────────────────────────────────────────────────────────────
IS_WINDOWS = (os.name == "nt")
IS_POSIX   = (os.name == "posix")
PLATFORM   = platform.system().lower()  # 'windows','linux','darwin'

# ─── Paths & Constants ───────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH  = os.path.join(SCRIPT_DIR, "config.json")
VENV_FLAG    = "--in-venv"
DEFAULT_VENV = os.path.join(SCRIPT_DIR, ".venv")  # short & local for POSIX/mac
HTTPS_PORT   = 443
PORT_TRIES   = 100
CERT_DIR     = os.path.join(SCRIPT_DIR, "certs")

# Globals for cleanup
_active_httpd = None
_child_proc   = None

# ─── Helpers ─────────────────────────────────────────────────────────────────
def _short_hash(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()[:10]

def _ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True); return path

def _venv_bin(venv_dir: str) -> str:
    return os.path.join(venv_dir, "Scripts" if IS_WINDOWS else "bin")

def _path_len(p: str) -> int:
    try: return len(os.path.abspath(p))
    except Exception: return len(p)

# Spinner for nicer UX
class Spinner:
    def __init__(self, msg):
        self.msg = msg
        self.spin = itertools.cycle("|/-\\")
        self._stop = threading.Event()
        self._thr = threading.Thread(target=self._run, daemon=True)
    def _run(self):
        while not self._stop.is_set():
            sys.stdout.write(f"\r{self.msg} {next(self.spin)}")
            sys.stdout.flush()
            time.sleep(0.1)
        sys.stdout.write("\r" + " "*(len(self.msg)+2) + "\r"); sys.stdout.flush()
    def __enter__(self): self._thr.start()
    def __exit__(self, *a): self._stop.set(); self._thr.join()

# ─── Console-safe input (this is the key Windows fix) ────────────────────────
def _readline_console(prompt: str) -> str:
    """
    Read from the REAL console device so Enter is captured even if stdin is weird.
    Windows:  CONIN$
    POSIX:    /dev/tty
    Falls back to input() if unavailable.
    """
    dev = "CONIN$" if IS_WINDOWS else "/dev/tty"
    try:
        sys.stdout.write(prompt); sys.stdout.flush()
        with open(dev, "r", encoding="utf-8", errors="ignore") as con:
            return con.readline()
    except Exception:
        # Last resort; may raise EOFError if stdin is redirected/closed
        return input(prompt)

def ask(prompt: str, *, default=None, required=False, cast=str, validate=None):
    """
    Robust prompt using _readline_console():
    - Enter accepts default (if provided) or "" (if not required).
    - required=True forces non-empty.
    - Optional cast/validate.
    """
    suffix = f" [{default}]" if default is not None else ""
    while True:
        try:
            s = _readline_console(f"{prompt}{suffix}: ")
        except EOFError:
            s = ""  # treat EOF as blank
        s = (s or "").strip()

        if not s:
            if default is not None:
                value = default
            elif required:
                print("Please enter a value.")
                continue
            else:
                value = ""
        else:
            value = s

        try:
            value = cast(value)
            if validate and not validate(value):
                print("Invalid value.")
                continue
            return value
        except ValueError as e:
            print(str(e) or "Invalid format. Try again.")

def ask_yes_no(prompt: str, *, default: bool | None = None) -> bool:
    """
    Y/N prompt using console. default=None forces explicit answer.
    """
    if default is True:  hint = " [Y/n]"
    elif default is False: hint = " [y/N]"
    else: hint = " [y/n]"
    while True:
        s = ( _readline_console(f"{prompt}{hint}: ") or "" ).strip().lower()
        if not s and default is not None:
            return default
        if s in ("y", "yes"): return True
        if s in ("n", "no"):  return False
        print("Please answer y or n.")

# ─── Venv bootstrap (Windows-safe path) ──────────────────────────────────────
def _choose_venv_dir() -> str:
    # On Windows, avoid long/spacey script paths for pip reliability
    if IS_WINDOWS:
        long_or_spacey = (_path_len(SCRIPT_DIR) > 90) or (" " in SCRIPT_DIR)
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("TEMP") or SCRIPT_DIR
        if long_or_spacey and base:
            bucket = _ensure_dir(os.path.join(base, "pyvenvs"))
            folder = f"{os.path.basename(SCRIPT_DIR) or 'app'}-{_short_hash(SCRIPT_DIR)}"
            return os.path.join(bucket, folder, "venv")
    return DEFAULT_VENV

def _exec_in_venv(venv_dir: str):
    py = os.path.join(_venv_bin(venv_dir), "python.exe" if IS_WINDOWS else "python")
    os.execv(py, [py, __file__, VENV_FLAG] + sys.argv[1:])

def bootstrap_and_run():
    if VENV_FLAG not in sys.argv:
        venv_dir = _choose_venv_dir()
        if not os.path.isdir(venv_dir):
            with Spinner(f"Creating virtualenv at {venv_dir}…"):
                subprocess.check_call([sys.executable, "-m", "venv", venv_dir])

        py = os.path.join(_venv_bin(venv_dir), "python.exe" if IS_WINDOWS else "python")

        with Spinner("Upgrading pip/setuptools/wheel…"):
            subprocess.check_call([py, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])

        with Spinner("Installing dependencies (cryptography)…"):
            subprocess.check_call([py, "-m", "pip", "install", "--upgrade", "cryptography"])

        _exec_in_venv(venv_dir)
    else:
        try: sys.argv.remove(VENV_FLAG)
        except ValueError: pass
        main()

# ─── Config ──────────────────────────────────────────────────────────────────
def load_config():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "serve_path": os.getcwd(),
        "extra_dns_sans": [],
        "cert_mode": "self",    # self | letsencrypt | stepca | gcpca
        "domains": [],
        "email": "",
        "le_staging": False,
        # Step CA
        "stepca_url": "",
        "stepca_fingerprint": "",
        "stepca_provisioner": "",
        "stepca_token": "",
        # GCP CA
        "gcpca_pool": "",
        "gcpca_location": "",
        "gcpca_cert_id": "",
    }

def save_config(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4)

# ─── CLI ─────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="HTTPS dev server with flexible certificate sources.")
    p.add_argument("--cert-mode", choices=["self","letsencrypt","stepca","gcpca"], help="Certificate source/mode.")
    p.add_argument("--domains", help="Comma-separated domain list (for LE/Step/GCP).")
    p.add_argument("--email", help="Email for Let's Encrypt.")
    p.add_argument("--agree-tos", action="store_true", help="Agree to Let's Encrypt TOS (required for LE).")
    p.add_argument("--le-staging", action="store_true", help="Use Let's Encrypt staging.")
    p.add_argument("--http01-port", type=int, default=80, help="Port for LE standalone HTTP-01.")
    # Step CA
    p.add_argument("--stepca-url")
    p.add_argument("--stepca-fingerprint")
    p.add_argument("--stepca-provisioner")
    p.add_argument("--stepca-token")
    # GCP CA
    p.add_argument("--gcpca-pool")
    p.add_argument("--gcpca-location")
    p.add_argument("--gcpca-cert-id")
    # Misc
    p.add_argument("--renew", action="store_true", help="Renew certificates (LE) and exit.")
    return p.parse_args()

# ─── Net helpers ─────────────────────────────────────────────────────────────
def get_lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("192.0.2.1", 80))  # No packets sent; chooses interface
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

def get_public_ip(timeout=3):
    try:
        return urllib.request.urlopen("https://api.ipify.org", timeout=timeout).read().decode().strip()
    except Exception:
        return None

def wait_for_listen(port, host="127.0.0.1", timeout_s=8.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.15)
    return False

# ─── Banner ──────────────────────────────────────────────────────────────────
def print_banner(port):
    lan    = get_lan_ip()
    public = get_public_ip()
    lines = [
        f"  Local : https://{lan}:{port}",
        f"  Public: https://{public}:{port}" if public else "  Public: <none>",
    ]
    w = max(len(l) for l in lines) + 4
    print("\n╔" + "═"*w + "╗")
    for l in lines: print("║" + l.ljust(w) + "║")
    print("╚" + "═"*w + "╝\n")

# ─── Certificates ────────────────────────────────────────────────────────────
def generate_self_signed(cert_file, key_file, extra_dns_sans):
    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509 import NameOID, SubjectAlternativeName, DNSName, IPAddress
    import cryptography.x509 as x509
    import ipaddress as ipa

    lan_ip    = get_lan_ip()
    public_ip = get_public_ip()

    keyobj = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    san_list = [DNSName("localhost"), IPAddress(ipa.ip_address("127.0.0.1"))]
    for ip in (lan_ip, public_ip):
        if ip:
            try: san_list.append(IPAddress(ipa.ip_address(ip)))
            except ValueError: pass
    for host in (extra_dns_sans or []):
        host = str(host).strip()
        if host: san_list.append(DNSName(host))

    san  = SubjectAlternativeName(san_list)
    cn   = lan_ip or "localhost"
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])

    not_before = datetime.now(timezone.utc) - timedelta(minutes=5)
    not_after  = not_before + timedelta(days=365)

    with Spinner("Generating self-signed certificate…"):
        cert = (x509.CertificateBuilder()
                .subject_name(name).issuer_name(name)
                .public_key(keyobj.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(not_before).not_valid_after(not_after)
                .add_extension(san, critical=False)
                .sign(keyobj, hashes.SHA256()))

    with open(key_file, "wb") as f:
        f.write(keyobj.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()))
    with open(cert_file, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

def ensure_le_cert(domains, email, agree_tos, staging, http01_port):
    if not domains or not email or not agree_tos:
        raise SystemExit("Let's Encrypt requires --domains, --email and --agree-tos.")
    if shutil.which("certbot") is None:
        raise SystemExit("certbot not found in PATH. Install Certbot.")
    primary = domains[0]
    live_dir = os.path.join("/etc/letsencrypt/live", primary)
    fullchain = os.path.join(live_dir, "fullchain.pem")
    privkey   = os.path.join(live_dir, "privkey.pem")

    if not (os.path.exists(fullchain) and os.path.exists(privkey)):
        # Best effort pre-bind check for standalone
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", http01_port))
            s.close()
        except OSError:
            print(f"⚠ Port {http01_port} appears busy; Certbot standalone may fail.")
        cmd = ["certbot", "certonly", "--standalone",
               "--non-interactive", "--agree-tos", "-m", email,
               "--preferred-challenges", "http",
               "--http-01-port", str(http01_port)]
        if staging: cmd.append("--staging")
        for d in domains: cmd += ["-d", d]
        with Spinner("Requesting Let's Encrypt certificate…"):
            subprocess.check_call(cmd)

    if not (os.path.exists(fullchain) and os.path.exists(privkey)):
        raise SystemExit("LE did not produce expected files in /etc/letsencrypt/live/<domain>.")
    return os.path.abspath(fullchain), os.path.abspath(privkey)

def renew_le_and_exit():
    if shutil.which("certbot") is None:
        raise SystemExit("certbot not found in PATH.")
    with Spinner("Renewing Let's Encrypt certificates…"):
        subprocess.check_call(["certbot", "renew"])
    print("✔ Renew complete."); sys.exit(0)

def ensure_stepca_cert(domains, url, fp, provisioner, token):
    if shutil.which("step") is None:
        raise SystemExit("step (Smallstep CLI) not found in PATH.")
    if not domains:
        raise SystemExit("Step CA requires --domains.")
    primary = domains[0]
    out_dir = _ensure_dir(os.path.join(CERT_DIR, "stepca", primary))
    cert_file = os.path.join(out_dir, "cert.pem")
    key_file  = os.path.join(out_dir, "key.pem")

    if url and fp:
        with Spinner("Bootstrapping Step CA…"):
            subprocess.check_call(["step", "ca", "bootstrap", "--ca-url", url, "--fingerprint", fp])

    cmd = ["step", "ca", "certificate", primary, cert_file, key_file, "--force"]
    if provisioner: cmd += ["--provisioner", provisioner]
    if token:       cmd += ["--token", token]
    with Spinner("Requesting certificate from Step CA…"):
        subprocess.check_call(cmd)

    if not (os.path.exists(cert_file) and os.path.exists(key_file)):
        raise SystemExit("Step CA did not produce expected cert/key files.")
    return os.path.abspath(cert_file), os.path.abspath(key_file)

def ensure_gcpca_cert(domains, pool, location, cert_id):
    if shutil.which("gcloud") is None:
        raise SystemExit("gcloud CLI not found. Install and run `gcloud init`.")
    if not domains or not pool or not location:
        raise SystemExit("GCP CA requires --domains, --gcpca-pool, and --gcpca-location.")
    primary = domains[0]
    out_dir = _ensure_dir(os.path.join(CERT_DIR, "gcpca", primary))
    cert_file = os.path.join(out_dir, "cert.pem")
    key_file  = os.path.join(out_dir, "key.pem")

    if not cert_id:
        cert_id = f"{primary.replace('.','-')}-{int(time.time())}"

    cmd = [
        "gcloud", "privateca", "certificates", "create", cert_id,
        f"--issuer-pool={pool}",
        f"--location={location}",
        f"--dns-san={primary}",
        "--generate-key",
        f"--key-output-file={key_file}",
        f"--pem-output-file={cert_file}",
    ]
    if len(domains) > 1:
        for d in domains[1:]:
            cmd.append(f"--dns-san={d}")

    with Spinner("Requesting certificate from Google Cloud Private CA…"):
        subprocess.check_call(cmd)

    if not (os.path.exists(cert_file) and os.path.exists(key_file)):
        raise SystemExit("GCP CA did not produce expected cert/key files.")
    return os.path.abspath(cert_file), os.path.abspath(key_file)

def resolve_domains(arg_domains, cfg_domains):
    if arg_domains:
        return [d.strip() for d in arg_domains.split(",") if d.strip()]
    return list(cfg_domains or [])

def ensure_certificates(cert_mode, cfg, args):
    if cert_mode == "self":
        cert_file = os.path.join(os.getcwd(), "cert.pem")
        key_file  = os.path.join(os.getcwd(), "key.pem")
        generate_self_signed(cert_file, key_file, cfg.get("extra_dns_sans"))
        return cert_file, key_file

    domains = resolve_domains(args.domains, cfg.get("domains", []))

    if cert_mode == "letsencrypt":
        return ensure_le_cert(
            domains=domains,
            email=(args.email or cfg.get("email") or ""),
            agree_tos=bool(args.agree_tos),
            staging=bool(args.le_staging or cfg.get("le_staging")),
            http01_port=int(args.http01_port),
        )
    if cert_mode == "stepca":
        return ensure_stepca_cert(
            domains=domains,
            url=args.stepca_url or cfg.get("stepca_url") or "",
            fp=args.stepca_fingerprint or cfg.get("stepca_fingerprint") or "",
            provisioner=args.stepca_provisioner or cfg.get("stepca_provisioner") or "",
            token=args.stepca_token or cfg.get("stepca_token") or "",
        )
    if cert_mode == "gcpca":
        return ensure_gcpca_cert(
            domains=domains,
            pool=args.gcpca_pool or cfg.get("gcpca_pool") or "",
            location=args.gcpca_location or cfg.get("gcpca_location") or "",
            cert_id=args.gcpca_cert_id or cfg.get("gcpca_cert_id") or "",
        )
    raise SystemExit(f"Unknown cert mode: {cert_mode}")

# ─── Signals / Cleanup ───────────────────────────────────────────────────────
def _shutdown_httpd(httpd, *, wait=True, timeout=3.0):
    # Run shutdown off-thread so signal handlers don't deadlock serve_forever().
    def _do_shutdown():
        try: httpd.shutdown()
        except Exception: pass
        try: httpd.server_close()
        except Exception: pass

    t = threading.Thread(target=_do_shutdown, daemon=True)
    t.start()
    if wait:
        t.join(timeout)

def _cleanup(*, wait_httpd=True):
    global _active_httpd, _child_proc
    httpd = _active_httpd
    _active_httpd = None
    if httpd is not None:
        _shutdown_httpd(httpd, wait=wait_httpd)
    if _child_proc is not None:
        try: _child_proc.terminate()
        except Exception: pass
        try: _child_proc.wait(timeout=3)
        except Exception: pass
        _child_proc = None

atexit.register(_cleanup)

def _signal_handler(signum, frame):
    _cleanup(wait_httpd=False)
    if signum == getattr(signal, "SIGTSTP", None):
        try:
            signal.signal(signal.SIGTSTP, signal.SIG_DFL)
            os.kill(os.getpid(), signal.SIGTSTP)
        except Exception:
            pass
        return
    sys.exit(0)

def install_signal_handlers():
    signal.signal(signal.SIGINT,  _signal_handler)
    if hasattr(signal, "SIGTSTP"): signal.signal(signal.SIGTSTP, _signal_handler)
    if hasattr(signal, "SIGTERM"): signal.signal(signal.SIGTERM, _signal_handler)
    if hasattr(signal, "SIGHUP"):  signal.signal(signal.SIGHUP,  _signal_handler)
    if hasattr(signal, "SIGQUIT"): signal.signal(signal.SIGQUIT, _signal_handler)

# ─── HTTPS Server (Python) ───────────────────────────────────────────────────
class ReusableHTTPSServer(HTTPServer):
    allow_reuse_address = True

def bind_https_server(context, start_port=HTTPS_PORT, tries=PORT_TRIES):
    last_err = None
    for p in range(start_port, start_port + tries):
        try:
            httpd = ReusableHTTPSServer(("0.0.0.0", p), SimpleHTTPRequestHandler)
            httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
            return httpd, p
        except OSError as e:
            last_err = e
            continue
    raise RuntimeError(f"Unable to bind any port in {start_port}..{start_port+tries-1} (last: {last_err})")

# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    install_signal_handlers()
    args = parse_args()

    # POSIX needs root for :443; re-exec with sudo.
    if IS_POSIX and hasattr(os, "geteuid") and os.geteuid() != 0:
        print("⚠ Need root to bind port 443; re-running with sudo…")
        os.execvp("sudo", ["sudo", sys.executable] + sys.argv)

    cfg = load_config()
    updated = False

    # First-run: robust interactive config
    if not os.path.exists(CONFIG_PATH):
        default_path = cfg.get("serve_path") or os.getcwd()
        serve_path   = ask("Serve path", default=default_path, required=False)
        if not serve_path:
            serve_path = default_path
        cfg["serve_path"] = serve_path

        extra = ask("Extra DNS SANs (comma-separated, optional)", default="", required=False)
        cfg["extra_dns_sans"] = [h.strip() for h in extra.split(",") if h.strip()] if extra else []
        updated = True

    # Persist CLI overrides for convenience
    if args.cert_mode:          cfg["cert_mode"]         = args.cert_mode;           updated = True
    if args.domains:            cfg["domains"]           = resolve_domains(args.domains, cfg.get("domains")); updated = True
    if args.email:              cfg["email"]             = args.email;               updated = True
    if args.le_staging:         cfg["le_staging"]        = True;                     updated = True
    if args.stepca_url:         cfg["stepca_url"]        = args.stepca_url;          updated = True
    if args.stepca_fingerprint: cfg["stepca_fingerprint"]= args.stepca_fingerprint;  updated = True
    if args.stepca_provisioner: cfg["stepca_provisioner"]= args.stepca_provisioner;  updated = True
    if args.stepca_token:       cfg["stepca_token"]      = args.stepca_token;        updated = True
    if args.gcpca_pool:         cfg["gcpca_pool"]        = args.gcpca_pool;          updated = True
    if args.gcpca_location:     cfg["gcpca_location"]    = args.gcpca_location;      updated = True
    if args.gcpca_cert_id:      cfg["gcpca_cert_id"]     = args.gcpca_cert_id;       updated = True

    if updated: save_config(cfg)

    # LE renew-only path
    if args.renew:
        renew_le_and_exit()

    # cd into serve directory
    if not os.path.isdir(cfg["serve_path"]):
        raise SystemExit(f"Serve path does not exist: {cfg['serve_path']}")
    os.chdir(cfg["serve_path"])

    # Obtain cert/key
    cert_mode = cfg.get("cert_mode", "self")
    cert_file, key_file = ensure_certificates(cert_mode, cfg, args)

    # SSL context
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.options |= ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3
    context.load_cert_chain(certfile=cert_file, keyfile=key_file)

    # If there's a Node app, force TLS via simple patch
    node_path = shutil.which("node")
    if os.path.exists("package.json") and os.path.exists("server.js") and node_path:
        patch_path = os.path.join(os.getcwd(), "tls_patch.js")
        CERT_ABS = os.path.abspath(cert_file)
        KEY_ABS  = os.path.abspath(key_file)
        with open(patch_path, "w", encoding="utf-8") as f:
            f.write(f"""\
const fs = require('fs');
const https = require('https');
const http = require('http');
const CERT = {json.dumps(CERT_ABS)};
const KEY  = {json.dumps(KEY_ABS)};

// Force HTTPS when app uses http.createServer(...)
const _create = http.createServer;
http.createServer = function (opts, listener) {{
  if (typeof opts === 'function') listener = opts;
  return https.createServer({{ key: fs.readFileSync(KEY), cert: fs.readFileSync(CERT) }}, listener);
}};
const _Server = http.Server;
http.Server = function (...args) {{
  return https.Server({{ key: fs.readFileSync(KEY), cert: fs.readFileSync(CERT) }}, ...args);
}};
http.Server.prototype = _Server.prototype;
""")

        env = os.environ.copy()
        env["PORT"] = str(HTTPS_PORT)  # many apps respect PORT
        cmd = [node_path, "-r", patch_path, "server.js"]

        global _child_proc
        with Spinner(f"Starting Node.js (TLS; target port {HTTPS_PORT})…"):
            _child_proc = subprocess.Popen(cmd, env=env, cwd=os.getcwd())

        if wait_for_listen(HTTPS_PORT, host="127.0.0.1", timeout_s=10.0):
            print_banner(HTTPS_PORT)
        else:
            print("⚠ Node app started, but port 443 not detected listening. Check app logs.")
        try:
            _child_proc.wait()
        except KeyboardInterrupt:
            pass
        finally:
            _cleanup()
        return

    # Python HTTPS fallback
    try:
        httpd, port = bind_https_server(context, start_port=HTTPS_PORT, tries=PORT_TRIES)
    except RuntimeError as e:
        raise SystemExit(str(e))

    global _active_httpd
    _active_httpd = httpd

    if port != HTTPS_PORT:
        print(f"⚠ Port {HTTPS_PORT} unavailable; selected free port {port}")

    print(f"→ Serving HTTPS from {os.getcwd()} on 0.0.0.0:{port}")
    print_banner(port)

    try:
        httpd.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        _cleanup()

# ─── Entrypoint ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    bootstrap_and_run()
