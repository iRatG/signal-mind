# Show VPN status: tunnel state, direct IP, IP-through-tunnel.
# Usage:  .\vpn\status.ps1
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
& .\.venv\Scripts\python.exe -c @"
from src.utils.proxy import is_vpn_up, external_ip
import requests
up = is_vpn_up()
print(f'VPN up:      {up}')
try:
    print(f'Direct IP:   {requests.get(\"https://api.ipify.org\", timeout=10).text.strip()}')
except Exception as e:
    print(f'Direct IP:   FAILED ({e})')
if up:
    try:
        print(f'Tunnel IP:   {external_ip(through_vpn=True)}')
    except Exception as e:
        print(f'Tunnel IP:   FAILED ({e})')
"@
