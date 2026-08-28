Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$testId = [guid]::NewGuid().ToString("N")
$version = "contract-test-$testId"
$outputRelative = ".artifact-test-$testId"
$output = [System.IO.Path]::GetFullPath((Join-Path $root $outputRelative))
$dirtyMarker = Join-Path $output ".dirty-source-marker"
if (-not $output.StartsWith("$root$([System.IO.Path]::DirectorySeparatorChar)", [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Unsafe test output path: $output"
}

$primaryError = $null
try {
    New-Item -ItemType Directory -Path $output -Force | Out-Null
    Set-Content -LiteralPath $dirtyMarker -Value "contract test only" -Encoding UTF8
    & (Join-Path $root "services\deploy\build_deploy_package.ps1") `
        -Version $version -OutputDir $outputRelative -Environment development -AllowDirtySource
    $staging = Join-Path $output "development\$version"
    $smartRoutingRequired = @(
        "smart_routing\__init__.py",
        "smart_routing\area_map.py",
        "smart_routing\census_geocoder.py",
        "smart_routing\common_vrp_api_server.py",
        "smart_routing\common_vrp_db.py",
        "smart_routing\common_vrp_runtime.py",
        "smart_routing\data_catalog.py",
        "smart_routing\geocode_storage.py",
        "smart_routing\google_geocoder.py",
        "smart_routing\here_geocoder.py",
        "smart_routing\live_atlanta_runtime.py",
        "smart_routing\nominatim_geocoder.py",
        "smart_routing\osrm_routing.py",
        "smart_routing\production_atlanta.py",
        "smart_routing\region_design.py",
        "smart_routing\region_sweep.py",
        "smart_routing\routing_policy_catalog.py",
        "smart_routing\routing_compare.py",
        "smart_routing\service_preprocess.py",
        "smart_routing\us_geocode_cleaner.py",
        "smart_routing\vrp_api_common.py",
        "smart_routing\vrp_api_server.py",
        "smart_routing\vrp_api_service.py",
        "smart_routing\vrp_mode_na_general.py",
        "smart_routing\vrp_mode_z_weekend.py",
        "smart_routing\production_assign_atlanta.py",
        "smart_routing\production_assign_atlanta_vrp.py"
    )
    $required = @(
        "requirements.txt",
        "deploy_manifest.json",
        "runtime_env.sh",
        "sr_common_vrp_api_server.py",
        "sr_common_vrp_client_server.py",
        "sr_vrp_api_server.py",
        "restart_smart_routing_api.sh",
        "start_common_vrp_dev.sh",
        "start_common_vrp_client_server_dev.sh",
        "bootstrap_common_vrp_dev.sh",
        "verify_deployment.py",
        "config\config.template.json",
        "config\common_vrp.dev.template.json",
        "config\data_catalog.json",
        "services\__init__.py",
        "services\api\__init__.py",
        "services\api\common_vrp_config.py",
        "services\api\region_plan_repository_v2.py",
        "services\api\region_plan_v2.py",
        "services\api\run_common_vrp_api.py",
        "services\api\sr_vrp_api_server.py",
        "tools\__init__.py",
        "tools\data\__init__.py",
        "tools\data\region_plan_workflow_v2.py",
        "systemd\common-vrp-dev.service",
        "systemd\common-vrp-client-dev.service",
        "systemd\smart-routing-dev.service"
    ) + $smartRoutingRequired
    foreach ($relative in $required) {
        if (-not (Test-Path -LiteralPath (Join-Path $staging $relative) -PathType Leaf)) {
            throw "Artifact contract missing: $relative"
        }
    }
    $unexpected = Get-ChildItem -LiteralPath (Join-Path $staging "config") -File -Filter "*.json" |
        Where-Object { $_.Name -notlike "*.template.json" -and $_.Name -ne "data_catalog.json" }
    if ($unexpected) {
        throw "Artifact contains runtime config: $($unexpected.Name -join ', ')"
    }
    $excluded = @(
        "config\common_vrp.prod.template.json",
        "start_common_vrp_prod.sh",
        "start_common_vrp_client_server_prod.sh",
        "systemd\common-vrp.service",
        "systemd\common-vrp-client.service",
        "systemd\smart-routing.service",
        "admin_tools",
        "services\db",
        "services\deploy",
        "services\tests",
        "osrm",
        "prompts",
        "data",
        "config\server_ftp.local.json",
        "sr_area_map.py",
        "sr_common_vrp_client.py",
        "smart_routing\area_map_usa.py",
        "smart_routing\asia_geocode_cleaner.py",
        "smart_routing\bigquery_runtime.py",
        "smart_routing\export_daily_stats.py",
        "smart_routing\prewarm_map_cache.py",
        "smart_routing\production_assign_atlanta_osrm.py",
        "smart_routing\profile_sync.py",
        "smart_routing\vrp_api_client.py",
        "smart_routing\select_data.sql"
    )
    foreach ($relative in $excluded) {
        if (Test-Path -LiteralPath (Join-Path $staging $relative)) {
            throw "Server runtime artifact contains development/operations file: $relative"
        }
    }
    $actualTools = Get-ChildItem -LiteralPath (Join-Path $staging "tools") -Recurse -File |
        ForEach-Object { $_.FullName.Substring($staging.Length + 1).Replace("/", "\") } |
        Sort-Object
    $expectedTools = @(
        "tools\__init__.py",
        "tools\data\__init__.py",
        "tools\data\region_plan_workflow_v2.py"
    ) | Sort-Object
    if (Compare-Object -ReferenceObject $expectedTools -DifferenceObject $actualTools) {
        throw "Runtime tools subset differs from the explicit Region Plan v2 allowlist."
    }
    $actualServiceApi = Get-ChildItem -LiteralPath (Join-Path $staging "services\api") -File |
        ForEach-Object { "services\api\$($_.Name)" } |
        Sort-Object
    $expectedServiceApi = @(
        "services\api\__init__.py",
        "services\api\common_vrp_config.py",
        "services\api\region_plan_repository_v2.py",
        "services\api\region_plan_v2.py",
        "services\api\run_common_vrp_api.py",
        "services\api\sr_vrp_api_server.py"
    ) | Sort-Object
    if (Compare-Object -ReferenceObject $expectedServiceApi -DifferenceObject $actualServiceApi) {
        throw "Runtime services/api subset differs from the explicit server allowlist."
    }
    $builderSource = Get-Content -LiteralPath (Join-Path $root "services\deploy\build_deploy_package.ps1") -Raw
    if ($builderSource -notmatch "Runtime secret scan rejected") {
        throw "Runtime artifact builder does not contain a fail-closed secret scan."
    }
    $manifestPath = Join-Path $staging "deploy_manifest.json"
    $manifestBytes = [System.IO.File]::ReadAllBytes($manifestPath)
    if ($manifestBytes.Length -ge 3 -and $manifestBytes[0] -eq 0xEF -and $manifestBytes[1] -eq 0xBB -and $manifestBytes[2] -eq 0xBF) {
        throw "Artifact manifest must be UTF-8 without BOM."
    }
    $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    if ([string]::IsNullOrWhiteSpace([string]$manifest.source_revision)) {
        throw "Artifact manifest has no source_revision."
    }
    if ($manifest.source_dirty -ne $true) {
        throw "Dirty-source contract build must record source_dirty=true."
    }
    if ($manifest.source_mode -ne "worktree") {
        throw "Development contract build must record source_mode=worktree."
    }
    if ($manifest.promotable -ne $false) {
        throw "Development contract build must be non-promotable."
    }
    if ($manifest.target_environment -ne "development") {
        throw "Artifact target_environment must be development."
    }
    if ($manifest.target_root -ne "/home/csda/AI_Routing/development") {
        throw "Artifact target_root must use the csda server home."
    }
    if ($manifest.artifact_type -ne "server-runtime") {
        throw "Artifact type must be server-runtime."
    }
    if ($manifest.PSObject.Properties.Name -contains "source_root") {
        throw "Artifact manifest must not expose source_root."
    }
    $manifestText = Get-Content -LiteralPath $manifestPath -Raw
    if ($manifestText -match "server_ftp\.local\.json") {
        throw "Artifact manifest exposes the local FTP configuration path."
    }

    $actualSmartRouting = Get-ChildItem -LiteralPath (Join-Path $staging "smart_routing") -File |
        ForEach-Object { "smart_routing\$($_.Name)" } |
        Sort-Object
    $expectedSmartRouting = $smartRoutingRequired | Sort-Object
    if (Compare-Object -ReferenceObject $expectedSmartRouting -DifferenceObject $actualSmartRouting) {
        throw "smart_routing artifact differs from the explicit server allowlist."
    }

    $smokeConfig = Join-Path $staging "common_vrp.smoke.json"
    $smokeConfigJson = @{
        environment = "development"
        api = @{ host = "127.0.0.1"; port = 8066 }
        routing_api_url = "http://127.0.0.1:8066"
        database = @{
            host = "127.0.0.1"
            port = 5432
            dbname = "vrp_db_dev"
            user = "smoke"
            password = "test-only-password"
        }
    } | ConvertTo-Json -Depth 5
    [System.IO.File]::WriteAllText(
        $smokeConfig,
        $smokeConfigJson,
        (New-Object System.Text.UTF8Encoding($false))
    )
    $previousCommonConfig = $env:COMMON_VRP_CONFIG_PATH
    $previousPythonPath = $env:PYTHONPATH
    try {
        $env:COMMON_VRP_CONFIG_PATH = $smokeConfig
        $env:PYTHONPATH = $staging
        Push-Location $staging
        try {
            & python -c "import sr_common_vrp_api_server, sr_common_vrp_client_server, sr_vrp_api_server; import services.api.region_plan_repository_v2; import services.api.region_plan_v2; import tools.data.region_plan_workflow_v2; from smart_routing.vrp_api_service import _load_mode_handler; assert callable(_load_mode_handler('na_general')); assert callable(_load_mode_handler('z_weekend'))"
            if ($LASTEXITCODE -ne 0) {
                throw "Generated artifact import/dynamic-handler smoke test failed."
            }
        }
        finally {
            Pop-Location
        }
    }
    finally {
        $env:COMMON_VRP_CONFIG_PATH = $previousCommonConfig
        $env:PYTHONPATH = $previousPythonPath
        Remove-Item -LiteralPath $smokeConfig -Force -ErrorAction SilentlyContinue
    }
    Write-Host "Deploy artifact contract passed."
}
catch {
    $primaryError = $_
    throw
}
finally {
    if (Test-Path -LiteralPath $output) {
        $resolvedCleanupTarget = [System.IO.Path]::GetFullPath($output)
        if (
            $resolvedCleanupTarget -ne $output -or
            -not $resolvedCleanupTarget.StartsWith("$root$([System.IO.Path]::DirectorySeparatorChar)", [System.StringComparison]::OrdinalIgnoreCase)
        ) {
            $cleanupSafetyMessage = "Unsafe recursive cleanup target: $resolvedCleanupTarget"
            if ($null -ne $primaryError) {
                Write-Warning "$cleanupSafetyMessage Cleanup skipped to preserve the primary contract error."
            }
            else {
                throw $cleanupSafetyMessage
            }
        }
        else {
            $cleanupError = $null
            for ($attempt = 1; $attempt -le 8; $attempt++) {
                try {
                    Remove-Item -LiteralPath $resolvedCleanupTarget -Recurse -Force
                    break
                }
                catch {
                    $cleanupError = $_
                    if ($attempt -eq 8) { break }
                    Start-Sleep -Milliseconds 250
                }
            }
            if ($null -ne $cleanupError -and (Test-Path -LiteralPath $resolvedCleanupTarget)) {
                if ($null -ne $primaryError) {
                    Write-Warning "Artifact cleanup also failed after the contract error: $($cleanupError.Exception.Message)"
                }
                else {
                    throw $cleanupError
                }
            }
        }
    }
}
