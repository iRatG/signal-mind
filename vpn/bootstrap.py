"""
One-time SSH key setup for the VPN tunnel.

What it does:
  1. Generates ``~/.ssh/signal_mind_vpn`` (Ed25519 keypair) if absent,
     using the system ``ssh-keygen`` so file permissions are correct.
  2. Connects to the VPN server with the password from .env and
     appends the public key to ``~/.ssh/authorized_keys`` on the server.
  3. Verifies that key-based ``ssh`` works without a password.

After this runs successfully, ``vpn_password`` in .env is no longer
needed at runtime — you can clear it or leave it (file is in .gitignore).

Run once:
    .venv/Scripts/python -m vpn.bootstrap
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import paramiko
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("vpn_host")
USER = os.getenv("vpn_user", "root")
PASSWORD = os.getenv("vpn_password")
KEY_PATH = Path(os.path.expanduser(os.getenv("vpn_ssh_key", "~/.ssh/signal_mind_vpn")))


def _generate_key() -> None:
    if KEY_PATH.exists():
        print(f"[skip] key already exists: {KEY_PATH}")
        return
    KEY_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"[gen]  ssh-keygen -> {KEY_PATH}")
    r = subprocess.run(
        [
            "ssh-keygen",
            "-t", "ed25519",
            "-f", str(KEY_PATH),
            "-N", "",
            "-C", "signal_mind_vpn",
            "-q",
        ],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"ssh-keygen failed: {r.stderr.strip()}")


def _upload_pubkey() -> None:
    pub_file = KEY_PATH.with_suffix(KEY_PATH.suffix + ".pub")
    if not pub_file.exists():
        pub_file = Path(str(KEY_PATH) + ".pub")
    pub = pub_file.read_text().strip()

    print(f"[ssh]  connect {USER}@{HOST} (password auth, one-time)")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        HOST,
        username=USER,
        password=PASSWORD,
        timeout=15,
        allow_agent=False,
        look_for_keys=False,
    )

    # Use a heredoc-free, quote-safe approach: pass the key via stdin.
    cmd = (
        "set -e; "
        "mkdir -p ~/.ssh && chmod 700 ~/.ssh; "
        "touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys; "
        "key=$(cat); "
        'grep -qxF "$key" ~/.ssh/authorized_keys || echo "$key" >> ~/.ssh/authorized_keys; '
        'echo OK'
    )
    stdin, stdout, stderr = client.exec_command(cmd)
    stdin.write(pub + "\n")
    stdin.channel.shutdown_write()
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    rc = stdout.channel.recv_exit_status()
    client.close()

    if rc != 0 or out != "OK":
        raise RuntimeError(f"pubkey upload failed (rc={rc}): {err or out!r}")
    print("[ok]   pubkey installed on server")


def _verify_key_auth() -> None:
    print("[test] key-only ssh login (no password)")
    r = subprocess.run(
        [
            "ssh",
            "-i", str(KEY_PATH),
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "PasswordAuthentication=no",
            "-o", "ConnectTimeout=10",
            f"{USER}@{HOST}",
            "echo OK_FROM_SERVER",
        ],
        capture_output=True, text=True,
    )
    if r.returncode != 0 or "OK_FROM_SERVER" not in r.stdout:
        raise RuntimeError(
            f"key-auth verification failed (rc={r.returncode}): "
            f"{r.stderr.strip() or r.stdout.strip()}"
        )
    print("[ok]   key auth works")


def main() -> int:
    if not HOST or not PASSWORD:
        print("ERROR: vpn_host / vpn_password not set in .env", file=sys.stderr)
        return 2
    _generate_key()
    _upload_pubkey()
    _verify_key_auth()
    print(
        "\n  VPN bootstrap complete.\n"
        f"  Next: from src.utils.proxy import vpn; "
        "with vpn() as p: ...\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
