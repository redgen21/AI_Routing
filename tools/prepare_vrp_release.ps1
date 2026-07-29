param(
    [Parameter(Mandatory = $true)][string]$Version,
    [string]$CommitMessage = "Update VRP routing rules"
)

$ErrorActionPreference = "Stop"
$root = (Get-Location).Path

$allowed = @(
    "smart_routing/vrp_mode_z_weekend.py",
    "smart_routing/vrp_mode_na_general.py",
    "smart_routing/common_vrp_runtime.py",
    "smart_routing/common_vrp_db.py",
    "smart_routing/common_vrp_api_server.py",
    "smart_routing/production_assign_atlanta_vrp.py",
    "services/api/__init__.py",
    "services/api/region_plan_v2.py",
    "services/api/region_plan_repository_v2.py",
    "tests/test_routing_evaluation_contract.py",
    ".gitignore"
)

Write-Host "[1/4] Checking worktree scope..."
$status = @(git status --short)
$unexpected = @($status | ForEach-Object {
    if ($_.Length -lt 4) { return }
    $path = $_.Substring(3).Replace("\", "/")
    if ($path -notin $allowed -and $path -notlike ".artifact-*/*") { $path }
}) | Sort-Object -Unique
if ($unexpected.Count -gt 0) {
    throw "Unrelated or unreviewed changes exist. Resolve them before release: $($unexpected -join ', ')"
}

Write-Host "[2/4] Running routing regression tests..."
python -m unittest tests.test_routing_evaluation_contract tests.test_city_routing_policy tests.test_atlanta6_solver_policy tests.test_common_vrp_atlanta6_runtime_api
if ($LASTEXITCODE -ne 0) { throw "Routing tests failed; release stopped." }

Write-Host "[3/4] Creating release commit..."
git add -- $allowed
git diff --cached --quiet
if ($LASTEXITCODE -eq 0) { throw "No staged release changes found." }
git commit -m $CommitMessage
if ($LASTEXITCODE -ne 0) { throw "Git commit failed; artifact build stopped." }

Write-Host "[4/4] Building Development artifact..."
& powershell -NoProfile -ExecutionPolicy Bypass -File "services/deploy/build_deploy_package.ps1" `
    -Environment development -Version $Version
if ($LASTEXITCODE -ne 0) { throw "Development artifact build failed." }

Write-Host "VRP release prepared: $Version"
Write-Host "Next: validate the Development artifact, then build/upload Production separately."
