@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
start "Smart Routing Common" /min powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%run_common_mode.ps1"
start "Smart Routing General" /min powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%run_general_mode.ps1"
endlocal
