# Stop the VPN tunnel.
# Usage:  .\vpn\stop.ps1
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
& .\.venv\Scripts\python.exe -c "from src.utils.proxy import stop_vpn, is_vpn_up; stop_vpn(); print('VPN up:', is_vpn_up())"
