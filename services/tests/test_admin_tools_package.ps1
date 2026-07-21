Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$outputRelative = ".artifact-admin-test"
$output = [IO.Path]::GetFullPath((Join-Path $root $outputRelative))
if (-not $output.StartsWith("$root$([IO.Path]::DirectorySeparatorChar)", [StringComparison]::OrdinalIgnoreCase)) {
    throw "Unsafe test output path: $output"
}

try {
    New-Item -ItemType Directory -Path $output -Force | Out-Null
    Set-Content -LiteralPath (Join-Path $output ".dirty-source-marker") -Value "contract test only" -Encoding UTF8
    & (Join-Path $root "services\deploy\build_admin_tools_package.ps1") `
        -Version "contract-test" -OutputDir $outputRelative -AllowDirtySource
    $staging = Join-Path $output "admin_tools\contract-test"
    $archivePath = Join-Path $output "admin_tools\ai-routing-admin-tools-contract-test.zip"
    $manifestPath = Join-Path $staging "deploy_manifest.json"
    $manifestBefore = [IO.File]::ReadAllBytes($manifestPath)
    $archiveHashBefore = (Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash
    $stagingHashesBefore = @(
        Get-ChildItem -LiteralPath $staging -Recurse -File | ForEach-Object {
            $relative = $_.FullName.Substring($staging.Length + 1).Replace("\", "/")
            "${relative}:$((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash)"
        } | Sort-Object
    )
    $collisionRaised = $false
    try {
        & (Join-Path $root "services\deploy\build_admin_tools_package.ps1") `
            -Version "contract-test" -OutputDir $outputRelative -AllowDirtySource
    }
    catch {
        if ($_.Exception.Message -notmatch "output already exists") {
            throw
        }
        $collisionRaised = $true
    }
    if (-not $collisionRaised) {
        throw "Admin artifact builder did not reject an existing version output."
    }
    if ((Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash -ne $archiveHashBefore) {
        throw "Admin artifact collision attempt changed the existing ZIP."
    }
    if (-not [System.Linq.Enumerable]::SequenceEqual($manifestBefore, [IO.File]::ReadAllBytes($manifestPath))) {
        throw "Admin artifact collision attempt changed the existing manifest bytes."
    }
    $stagingHashesAfter = @(
        Get-ChildItem -LiteralPath $staging -Recurse -File | ForEach-Object {
            $relative = $_.FullName.Substring($staging.Length + 1).Replace("\", "/")
            "${relative}:$((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash)"
        } | Sort-Object
    )
    if (@(Compare-Object -ReferenceObject $stagingHashesBefore -DifferenceObject $stagingHashesAfter).Count -ne 0) {
        throw "Admin artifact collision attempt changed existing staging files."
    }
    foreach ($relative in @(
        "deploy_manifest.json",
        "requirements.txt",
        "admin_tools\__init__.py",
        "admin_tools\db\__init__.py",
        "admin_tools\db\guard.py",
        "admin_tools\db\master_data_backend.py",
        "admin_tools\db\release_backend.py",
        "admin_tools\db\common_vrp.py",
        "admin_tools\db\data_catalog.py",
        "admin_tools\db\heavy_repair.py",
        "admin_tools\db\README.md",
        "admin_tools\db\migrations\README.md",
        "admin_tools\db\runners\reset_common_vrp_data.py",
        "admin_tools\db\seeds\build_la_bucket_vrp_inputs.py",
        "config\common_vrp.dev.template.json",
        "config\common_vrp.prod.template.json",
        "config\data_catalog.admin.template.json"
    )) {
        if (-not (Test-Path -LiteralPath (Join-Path $staging $relative) -PathType Leaf)) {
            throw "Admin artifact contract missing: $relative"
        }
    }
    foreach ($relative in @(
        "config\config.json",
        "config\server_deploy.local.json",
        "services",
        "systemd",
        "data",
        "260310",
        "start_common_vrp_prod.sh",
        "smart_routing"
    )) {
        if (Test-Path -LiteralPath (Join-Path $staging $relative)) {
            throw "Admin artifact contains forbidden runtime/secret/data path: $relative"
        }
    }
    $manifest = Get-Content -LiteralPath (Join-Path $staging "deploy_manifest.json") -Raw | ConvertFrom-Json
    if ($manifest.artifact_type -ne "db-admin-tools") { throw "Unexpected admin artifact type." }
    if ($manifest.target_root -ne "/home/csda/AI_Routing/admin_tools/releases/contract-test") {
        throw "Unexpected admin target root."
    }
    if ($manifest.promotable -ne (-not [bool]$manifest.source_dirty)) {
        throw "Admin artifact promotable flag must be the inverse of source_dirty."
    }
    if ($manifest.contains_secrets -ne $false -or $manifest.contains_data -ne $false) {
        throw "Admin artifact must declare that it contains no secrets or data."
    }
    foreach ($templateName in @(
        "config\common_vrp.dev.template.json",
        "config\common_vrp.prod.template.json"
    )) {
        $template = Get-Content -LiteralPath (Join-Path $staging $templateName) -Raw | ConvertFrom-Json
        if ([string]$template.database.password -ne "<REPLACE_ME>") {
            throw "Admin artifact DB template must contain only the explicit password placeholder: $templateName"
        }
    }
    $localConfigs = @(Get-ChildItem -LiteralPath $staging -Recurse -File | Where-Object {
        $_.Name -match '(?i)\.local\.json$'
    })
    if ($localConfigs.Count -ne 0) {
        throw "Admin artifact contains an ignored local configuration file."
    }
    $packagedText = (Get-ChildItem -LiteralPath $staging -Recurse -File | Where-Object {
        @(".py", ".json", ".md", ".sql", ".txt") -contains $_.Extension.ToLowerInvariant()
    } | ForEach-Object { [IO.File]::ReadAllText($_.FullName) }) -join "`n"
    if ($packagedText -match '(?im)^\s*(?:from|import)\s+smart_routing(?:\.|\s|$)') {
        throw "Admin artifact must not import the application smart_routing package."
    }
    $builderSource = Get-Content -LiteralPath (Join-Path $root "services\deploy\build_admin_tools_package.ps1") -Raw
    if ($builderSource -notmatch "Admin-tools secret scan rejected") {
        throw "Admin artifact builder does not contain a fail-closed secret scan."
    }
    if ($builderSource -match "Copy-SourceTree") {
        throw "Admin artifact builder must not recursively copy source trees."
    }
    if ($builderSource -match '"smart_routing/') {
        throw "Admin artifact builder must not allowlist smart_routing paths."
    }
    $manifestPaths = @($manifest.files | ForEach-Object { [string]$_.path } | Sort-Object)
    if ($manifestPaths.Count -ne 22) {
        throw "Admin artifact manifest file count must be 22 before its self manifest entry."
    }
    foreach ($entry in $manifest.files) {
        $relative = [string]$entry.path
        $expectedHash = [string]$entry.sha256
        $actualHash = (Get-FileHash -LiteralPath (Join-Path $staging $relative) -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($expectedHash -notmatch '^[0-9a-f]{64}$' -or $actualHash -ne $expectedHash.ToLowerInvariant()) {
            throw "Admin artifact manifest checksum does not match its staged allowlisted file: $relative"
        }
    }
    $actualPaths = @(
        Get-ChildItem -LiteralPath $staging -Recurse -File | ForEach-Object {
            $_.FullName.Substring($staging.Length + 1).Replace("\", "/")
        } | Where-Object { $_ -ne "deploy_manifest.json" } | Sort-Object
    )
    if (@(Compare-Object -ReferenceObject $manifestPaths -DifferenceObject $actualPaths).Count -ne 0) {
        throw "Admin artifact staging contains a file outside its exact manifest allowlist."
    }
    if (@($actualPaths | Where-Object { $_ -like "smart_routing/*" }).Count -ne 0) {
        throw "Admin artifact staging must contain zero smart_routing paths."
    }
    if ($actualPaths.Count -ne 22) {
        throw "Admin artifact staging file count must be 22 before deploy_manifest.json."
    }
    $manifestBytes = [IO.File]::ReadAllBytes((Join-Path $staging "deploy_manifest.json"))
    if ($manifestBytes.Length -ge 3 -and $manifestBytes[0] -eq 0xEF -and $manifestBytes[1] -eq 0xBB -and $manifestBytes[2] -eq 0xBF) {
        throw "Admin artifact manifest must be UTF-8 without BOM."
    }
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [IO.Compression.ZipFile]::OpenRead($archivePath)
    try {
        $zipPaths = @($archive.Entries | Where-Object { -not $_.FullName.EndsWith("/") } | ForEach-Object {
            $_.FullName.TrimEnd("/").Replace("\", "/")
        } | Sort-Object)
        $expectedZipPaths = @($actualPaths)
        $expectedZipPaths += "deploy_manifest.json"
        $expectedZipPaths = @($expectedZipPaths | Sort-Object)
        if (@(Compare-Object -ReferenceObject $expectedZipPaths -DifferenceObject $zipPaths).Count -ne 0) {
            throw "Admin artifact ZIP does not exactly match staging plus deploy_manifest.json."
        }
        if (@($zipPaths | Where-Object { $_ -like "smart_routing/*" }).Count -ne 0) {
            throw "Admin artifact ZIP must contain zero smart_routing paths."
        }
        if ($zipPaths.Count -ne 23) {
            throw "Admin artifact ZIP must contain 23 files including deploy_manifest.json."
        }
    }
    finally {
        $archive.Dispose()
    }
    $previousPythonPath = $env:PYTHONPATH
    try {
        # Run from staging with no inherited module search path so these imports
        # cannot silently resolve to the source checkout.
        $env:PYTHONPATH = ""
        Push-Location -LiteralPath $staging
        try {
            & python -c "from pathlib import Path; import admin_tools.db.master_data_backend as backend; import admin_tools.db.common_vrp as common; import admin_tools.db.data_catalog as catalog; import admin_tools.db.heavy_repair as repair; import admin_tools.db.runners.reset_common_vrp_data as reset; import admin_tools.db.seeds.build_la_bucket_vrp_inputs as la; import admin_tools.db.seeds.import_asia_technician_centroids as asia; root=Path.cwd().resolve(); modules=(backend, common, catalog, repair, reset, la, asia); assert all(Path(module.__file__).resolve().is_relative_to(root) for module in modules); assert backend.CONTRACT_VERSION == 'db-admin/v1'; assert len(backend.TABLE_REGISTRY) == 13"
            if ($LASTEXITCODE -ne 0) {
                throw "Admin artifact entrypoint import smoke check failed."
            }
        }
        finally {
            Pop-Location
        }
    }
    finally {
        $env:PYTHONPATH = $previousPythonPath
    }

    Write-Host "Admin-tools artifact contract passed."
}
finally {
    if (Test-Path -LiteralPath $output) {
        for ($attempt = 1; $attempt -le 20; $attempt++) {
            try {
                Remove-Item -LiteralPath $output -Recurse -Force
                break
            }
            catch {
                if ($attempt -eq 20) {
                    Write-Warning "Could not remove antivirus-locked contract output: $output"
                    break
                }
                Start-Sleep -Milliseconds 250
            }
        }
    }
}
