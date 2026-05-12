"""
SSH SOCKS5 tunnel — VPN-style routing for parsing foreign sources
that the current system VPN blocks.

Usage:
    from src.utils.proxy import vpn, get_proxies
    import requests

    # context manager (recommended)
    with vpn() as proxies:
        r = requests.get("https://www.bbc.com/news", proxies=proxies)

    # or attach to an existing session
    session = requests.Session()
    session.proxies = get_proxies()

The first call auto-starts the tunnel (a background ``ssh -D`` process).
The tunnel stays up until the Python process exits (``atexit`` cleans it up)
or ``stop_vpn()`` is called explicitly.

Config is read from .env:
    vpn_host, vpn_user, vpn_ssh_key, vpn_local_port

The SSH key must already exist — run ``python -m vpn.bootstrap`` once to
generate it and install the pubkey on the server.
"""
from __future__ import annotations

import atexit
import os
import socket
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from dotenv import load_dotenv

load_dotenv()

_HOST = os.getenv("vpn_host")
_USER = os.getenv("vpn_user", "root")
_KEY = Path(os.path.expanduser(os.getenv("vpn_ssh_key", "~/.ssh/signal_mind_vpn")))
_PORT = int(os.getenv("vpn_local_port", "1080"))

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PID_FILE = _PROJECT_ROOT / "vpn" / ".tunnel.pid"

_CREATIONFLAGS = 0
if os.name == "nt":
    _CREATIONFLAGS = (
        subprocess.CREATE_NEW_PROCESS_GROUP | 0x00000008  # DETACHED_PROCESS
    )


def _port_open(host: str, port: int, timeout: float = 0.3) -> bool:
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def _pid_alive(pid: int) -> bool:
    if os.name == "nt":
        r = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True, text=True,
        )
        return str(pid) in r.stdout
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def is_vpn_up() -> bool:
    """True iff the local SOCKS5 port answers AND a tracked ssh PID is alive."""
    if not _port_open("127.0.0.1", _PORT):
        return False
    if not _PID_FILE.exists():
        # Port is busy but not by us — be safe, say no.
        return False
    try:
        pid = int(_PID_FILE.read_text().strip())
    except (OSError, ValueError):
        return False
    return _pid_alive(pid)


def start_vpn(timeout: float = 8.0) -> None:
    """Open the SSH SOCKS5 tunnel. No-op if already running."""
    if is_vpn_up():
        return
    if not _HOST:
        raise RuntimeError("vpn_host not set in .env")
    if not _KEY.exists():
        raise RuntimeError(
            f"SSH key not found at {_KEY}. "
            "Run `.venv/Scripts/python -m vpn.bootstrap` once to set up."
        )
    if _port_open("127.0.0.1", _PORT):
        raise RuntimeError(
            f"Local port {_PORT} is busy but not owned by this tunnel. "
            "Pick another vpn_local_port in .env or stop the other process."
        )

    _PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(
        [
            "ssh",
            "-i", str(_KEY),
            "-D", str(_PORT),
            "-N",
            "-q",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ServerAliveInterval=60",
            "-o", "ServerAliveCountMax=3",
            "-o", "ExitOnForwardFailure=yes",
            "-o", "BatchMode=yes",
            f"{_USER}@{_HOST}",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=_CREATIONFLAGS,
    )
    _PID_FILE.write_text(str(proc.pid))

    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            _PID_FILE.unlink(missing_ok=True)
            raise RuntimeError(
                f"ssh tunnel exited immediately (code {proc.returncode}). "
                f"Try: ssh -i {_KEY} -v {_USER}@{_HOST}"
            )
        if _port_open("127.0.0.1", _PORT):
            atexit.register(stop_vpn)
            return
        time.sleep(0.1)

    proc.terminate()
    _PID_FILE.unlink(missing_ok=True)
    raise RuntimeError(f"VPN tunnel did not come up within {timeout}s")


def stop_vpn() -> None:
    """Kill the tracked ssh tunnel process, if any."""
    if not _PID_FILE.exists():
        return
    try:
        pid = int(_PID_FILE.read_text().strip())
    except (OSError, ValueError):
        _PID_FILE.unlink(missing_ok=True)
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/F", "/PID", str(pid)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    else:
        try:
            os.kill(pid, 15)
        except OSError:
            pass
    _PID_FILE.unlink(missing_ok=True)


def get_proxies() -> dict[str, str]:
    """Return a ``proxies`` dict ready to pass to requests / httpx.

    Auto-starts the tunnel if not running. Uses ``socks5h://`` so DNS
    is resolved on the remote side (no leak through the local resolver).
    """
    if not is_vpn_up():
        start_vpn()
    url = f"socks5h://127.0.0.1:{_PORT}"
    return {"http": url, "https": url}


@contextmanager
def vpn() -> Iterator[dict[str, str]]:
    """
    Context manager. Tunnel is started if needed; it is NOT torn down on
    exit (long-lived; ``atexit`` cleans it up when the process ends), so
    repeated ``with vpn()`` blocks are cheap.

        with vpn() as proxies:
            requests.get(url, proxies=proxies)
    """
    yield get_proxies()


def external_ip(through_vpn: bool) -> str:
    """Helper for diagnostics: return the public IP seen by an external
    service, optionally through the VPN. Used by smoke tests."""
    import requests
    kwargs: dict = {"timeout": 10}
    if through_vpn:
        kwargs["proxies"] = get_proxies()
    return requests.get("https://api.ipify.org", **kwargs).text.strip()
