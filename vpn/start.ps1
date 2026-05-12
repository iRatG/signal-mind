# Manually start the VPN tunnel. No-op if already up.
# Usage:  .\vpn\start.ps1
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
& .\.venv\Scripts\python.exe -c "from src.utils.proxy import start_vpn, external_ip; start_vpn(); print('VPN up. External IP:', external_ip(through_vpn=True))"
