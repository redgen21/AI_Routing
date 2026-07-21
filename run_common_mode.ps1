Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = Resolve-Path (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $projectRoot

$configPath = Join-Path $projectRoot "config_common_vrp.json"
if (-not (Test-Path -LiteralPath $configPath -PathType Leaf)) {
    $configPath = Join-Path $projectRoot "config\common_vrp.prod.json"
}
if (-not (Test-Path -LiteralPath $configPath -PathType Leaf)) {
    throw "Missing production config: $configPath"
}
$streamlit = Join-Path $projectRoot ".venv\Scripts\streamlit.exe"
if (-not (Test-Path -LiteralPath $streamlit -PathType Leaf)) {
    $streamlit = (Get-Command streamlit -ErrorAction Stop).Source
}
$env:COMMON_VRP_CONFIG_PATH = $configPath
& $streamlit run sr_common_vrp_client.py --server.port 8501 @args
