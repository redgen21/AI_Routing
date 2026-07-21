Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = Resolve-Path (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $projectRoot

$streamlit = Join-Path $projectRoot ".venv\Scripts\streamlit.exe"
if (-not (Test-Path -LiteralPath $streamlit -PathType Leaf)) {
    $streamlit = (Get-Command streamlit -ErrorAction Stop).Source
}
& $streamlit run sr_vrp_api_client.py --server.port 8502 @args
