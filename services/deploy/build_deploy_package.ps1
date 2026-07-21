param(
    [string]$Version = "",
    [string]$OutputDir = "deployment",
    [ValidateSet("development", "production")]
    [string]$Environment = "production",
    [switch]$IncludeDocs,
    [switch]$AllowDirtySource
)

$ErrorActionPreference = "Stop"

function ConvertTo-NativeFileSystemPath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [switch]$MustExist
    )

    $Provider = $null
    $Drive = $null
    try {
        $NativePath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath(
            $Path,
            [ref]$Provider,
            [ref]$Drive
        )
    }
    catch {
        throw "Path must resolve through the filesystem provider: $Path"
    }
    if ($null -eq $Provider -or $Provider.Name -ne "FileSystem") {
        throw "Only filesystem paths are allowed: $Path"
    }
    $NativePath = [System.IO.Path]::GetFullPath([string]$NativePath)
    if (
        [string]::IsNullOrWhiteSpace($NativePath) -or
        $NativePath -match "^(?:Microsoft\.PowerShell\.Core\\)?FileSystem::" -or
        $NativePath -match "^[^\\/]+::"
    ) {
        throw "Provider-qualified paths are not valid external process paths: $NativePath"
    }
    if ($MustExist -and -not (Test-Path -LiteralPath $NativePath -PathType Container)) {
        throw "Required filesystem directory does not exist: $NativePath"
    }
    return [string]$NativePath
}

$Root = ConvertTo-NativeFileSystemPath `
    -Path (Join-Path -Path $PSScriptRoot -ChildPath "..\..") `
    -MustExist
$SourceRevision = [string](& git -C $Root rev-parse HEAD)
$RevisionExitCode = $LASTEXITCODE
$SourceRevision = $SourceRevision.Trim()
if ($RevisionExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($SourceRevision)) {
    throw "A Git checkout with a resolvable HEAD is required to build a deploy artifact."
}
$SourceStatus = @(& git -C $Root status --porcelain --untracked-files=all)
$StatusExitCode = $LASTEXITCODE
if ($StatusExitCode -ne 0) {
    throw "Unable to inspect Git source status; refusing to build."
}
$SourceDirty = $SourceStatus.Count -gt 0
$IsProduction = $Environment -eq "production"
if ($IsProduction -and $AllowDirtySource) {
    throw "Production artifacts cannot use -AllowDirtySource. Commit and verify a clean checkout first."
}
if ($SourceDirty -and -not $AllowDirtySource) {
    throw "Deploy source has tracked or untracked changes. Build from a clean checkout, or use -AllowDirtySource only for non-production verification."
}
if ([string]::IsNullOrWhiteSpace($Version)) {
    $Version = Get-Date -Format "yyyyMMdd-HHmmss"
}
if ($Version -notmatch "^[A-Za-z0-9][A-Za-z0-9._-]*$") {
    throw "Version may contain only letters, numbers, dot, underscore, and hyphen."
}

$PackageName = "ai-routing-runtime-$Environment-$Version"
$RootPath = [System.IO.Path]::GetFullPath("$Root").TrimEnd([System.IO.Path]::DirectorySeparatorChar)
$OutputRoot = [System.IO.Path]::GetFullPath((Join-Path $Root $OutputDir))
$EnvironmentOutputRoot = Join-Path $OutputRoot $Environment
$FinalStagingDir = Join-Path $EnvironmentOutputRoot $Version
$FinalZipPath = Join-Path $EnvironmentOutputRoot "$PackageName.zip"

if (-not $OutputRoot.StartsWith("$RootPath$([System.IO.Path]::DirectorySeparatorChar)", [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "OutputDir must resolve inside the repository root: $OutputRoot"
}

if (Test-Path $FinalStagingDir) {
    throw "Artifact staging already exists; choose a new version: $FinalStagingDir"
}
if (Test-Path $FinalZipPath) {
    throw "Artifact archive already exists; choose a new version: $FinalZipPath"
}

$SnapshotTempDir = $null
$BuildSessionDir = $null
$SourceRoot = $Root
try {
    if ($IsProduction) {
        # Freeze every production input to the exact commit validated above.  The
        # worktree is used only to invoke Git; package allowlists read the archive.
        $SnapshotTempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("ai-routing-runtime-snapshot-" + [guid]::NewGuid().ToString("N"))
        $SnapshotZip = Join-Path $SnapshotTempDir "source.zip"
        $SourceRoot = Join-Path $SnapshotTempDir "source"
        New-Item -ItemType Directory -Path $SourceRoot | Out-Null
        $GitArchiveArgs = @(
            "-C", [string]$Root,
            "archive",
            "--format=zip",
            "--output=$SnapshotZip",
            $SourceRevision
        )
        & git @GitArchiveArgs
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $SnapshotZip -PathType Leaf)) {
            throw "Unable to create immutable Git snapshot for production artifact."
        }
        Expand-Archive -LiteralPath $SnapshotZip -DestinationPath $SourceRoot
    }

    New-Item -ItemType Directory -Path $EnvironmentOutputRoot -Force | Out-Null
    $BuildingRoot = Join-Path $EnvironmentOutputRoot "_building"
    New-Item -ItemType Directory -Path $BuildingRoot -Force | Out-Null
    $BuildSessionDir = Join-Path $BuildingRoot ("runtime-" + [guid]::NewGuid().ToString("N"))
    $StagingDir = Join-Path $BuildSessionDir "staging"
    $ZipPath = Join-Path $BuildSessionDir "$PackageName.zip"
    New-Item -ItemType Directory -Path $BuildSessionDir | Out-Null
    New-Item -ItemType Directory -Path $StagingDir | Out-Null

function Get-PackageRelativePath {
    param([Parameter(Mandatory = $true)][string]$FullName)

    $rootPath = [System.IO.Path]::GetFullPath("$SourceRoot")
    if (-not $rootPath.EndsWith([System.IO.Path]::DirectorySeparatorChar)) {
        $rootPath += [System.IO.Path]::DirectorySeparatorChar
    }
    $filePath = [System.IO.Path]::GetFullPath($FullName)
    $rootUri = New-Object System.Uri($rootPath)
    $fileUri = New-Object System.Uri($filePath)
    return [System.Uri]::UnescapeDataString($rootUri.MakeRelativeUri($fileUri).ToString()).Replace("/", [System.IO.Path]::DirectorySeparatorChar)
}

function Get-RelativePathFromBase {
    param(
        [Parameter(Mandatory = $true)][string]$BasePath,
        [Parameter(Mandatory = $true)][string]$FullName
    )
    $normalizedBase = [System.IO.Path]::GetFullPath($BasePath)
    if (-not $normalizedBase.EndsWith([System.IO.Path]::DirectorySeparatorChar)) {
        $normalizedBase += [System.IO.Path]::DirectorySeparatorChar
    }
    $baseUri = New-Object System.Uri($normalizedBase)
    $fileUri = New-Object System.Uri([System.IO.Path]::GetFullPath($FullName))
    return [System.Uri]::UnescapeDataString($baseUri.MakeRelativeUri($fileUri).ToString()).Replace("/", "\")
}

function Move-PublishPathWithRetry {
    param(
        [Parameter(Mandatory = $true)][ValidateSet("File", "Directory")][string]$Kind,
        [Parameter(Mandatory = $true)][string]$SourcePath,
        [Parameter(Mandatory = $true)][string]$DestinationPath,
        [int]$MaxAttempts = 8,
        [int]$DelayMilliseconds = 250
    )

    for ($Attempt = 1; $Attempt -le $MaxAttempts; $Attempt++) {
        if (Test-Path -LiteralPath $DestinationPath) {
            throw "Publish destination already exists; refusing to overwrite: $DestinationPath"
        }
        try {
            if ($Kind -eq "File") {
                [System.IO.File]::Move($SourcePath, $DestinationPath)
            }
            else {
                [System.IO.Directory]::Move($SourcePath, $DestinationPath)
            }
            return
        }
        catch {
            if (Test-Path -LiteralPath $DestinationPath) {
                throw "Publish destination appeared during retry; refusing to overwrite: $DestinationPath"
            }
            if ($Attempt -eq $MaxAttempts) {
                throw
            }
            [System.GC]::Collect()
            [System.GC]::WaitForPendingFinalizers()
            Start-Sleep -Milliseconds ($DelayMilliseconds * $Attempt)
        }
    }
}

function Get-FileSha256WithRetry {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [int]$MaxAttempts = 8,
        [int]$DelayMilliseconds = 250
    )

    for ($Attempt = 1; $Attempt -le $MaxAttempts; $Attempt++) {
        try {
            return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash
        }
        catch {
            if ($Attempt -eq $MaxAttempts) {
                throw
            }
            [System.GC]::Collect()
            [System.GC]::WaitForPendingFinalizers()
            Start-Sleep -Milliseconds ($DelayMilliseconds * $Attempt)
        }
    }
}

function Copy-FileToPackage {
    param(
        [Parameter(Mandatory = $true)][string]$SourcePath,
        [Parameter(Mandatory = $true)][string]$RelativePath
    )
    $TargetPath = Join-Path $StagingDir $RelativePath
    $TargetParent = Split-Path $TargetPath -Parent
    if (-not (Test-Path $TargetParent)) {
        New-Item -ItemType Directory -Path $TargetParent -Force | Out-Null
    }
    Copy-Item -LiteralPath $SourcePath -Destination $TargetPath -Force
}

function Copy-RequiredFile {
    param(
        [Parameter(Mandatory = $true)][string]$SourceRelativePath,
        [Parameter(Mandatory = $true)][string]$OutputRelativePath
    )
    $SourcePath = Join-Path $SourceRoot $SourceRelativePath
    if (-not (Test-Path -LiteralPath $SourcePath -PathType Leaf)) {
        throw "Required deploy source is missing: $SourceRelativePath"
    }
    Copy-FileToPackage -SourcePath $SourcePath -RelativePath $OutputRelativePath
}

function Test-ExcludedFile {
    param([string]$RelativePath)

    $normalized = $RelativePath -replace "\\", "/"
    $name = Split-Path $RelativePath -Leaf

    if ($normalized -match "(^|/)(__pycache__|\.git|\.pytest_cache|\.streamlit|dist|log)(/|$)") { return $true }
    if ($normalized -match "(^|/)(260310|260507|260514|data)(/|$)") { return $true }
    if ($normalized -match "\.(pyc|pyo|pyd|log|tmp|bak)$") { return $true }
    if ($normalized -match "\.(csv|xlsx|xls|parquet)$") { return $true }
    if ($normalized -match "\.(osm|osm\.pbf|osrm|osrm\.fileIndex|osrm\.geometry|osrm\.hsgr|osrm\.mldgr|osrm\.names|osrm\.properties|osrm\.ramIndex|osrm\.timestamp|osrm\.turn_duration_penalties|osrm\.turn_penalties|osrm\.datasource_names)$") { return $true }
    if ($name -like "~$*") { return $true }
    return $false
}

function Copy-DirectoryFiltered {
    param(
        [Parameter(Mandatory = $true)][string]$DirectoryName,
        [string[]]$AllowedExtensions = @()
    )

    $SourceDir = Join-Path $SourceRoot $DirectoryName
    if (-not (Test-Path $SourceDir)) {
        return
    }

    Get-ChildItem -LiteralPath $SourceDir -Recurse -File | ForEach-Object {
        $relative = Get-PackageRelativePath $_.FullName
        if (Test-ExcludedFile $relative) {
            return
        }
        if ($AllowedExtensions.Count -gt 0 -and $AllowedExtensions -notcontains $_.Extension.ToLowerInvariant()) {
            return
        }
        Copy-FileToPackage -SourcePath $_.FullName -RelativePath $relative
    }
}

$CommonRuntimeFiles = @(
    "runtime_env.sh",
    "restart_smart_routing_api.sh",
    "sr_common_vrp_api_server.py",
    "sr_common_vrp_client_server.py",
    "sr_vrp_api_server.py",
    "verify_deployment.py"
)
$ProductionRuntimeFiles = @(
    "restart_common_vrp_api.sh",
    "restart_common_vrp_client_server.sh",
    "start_common_vrp_client_server_prod.sh",
    "start_common_vrp_prod.sh"
)
$DevelopmentRuntimeFiles = @(
    "bootstrap_common_vrp_dev.sh",
    "start_common_vrp_client_server_dev.sh",
    "start_common_vrp_dev.sh"
)
$EnvironmentRuntimeFiles = if ($IsProduction) { $ProductionRuntimeFiles } else { $DevelopmentRuntimeFiles }
$RuntimeRootFiles = @($CommonRuntimeFiles) + @($EnvironmentRuntimeFiles)
foreach ($relativePath in $RuntimeRootFiles) {
    Copy-RequiredFile -SourceRelativePath $relativePath -OutputRelativePath $relativePath
}

# Keep the server artifact fail-closed: every core module must be named here.
# vrp_api_service imports the mode handlers dynamically, so those handlers and
# their implementation modules are explicit even though a static import walk
# cannot discover them.
$SmartRoutingRuntimeFiles = @(
    "smart_routing/__init__.py",
    "smart_routing/area_map.py",
    "smart_routing/census_geocoder.py",
    "smart_routing/common_vrp_api_server.py",
    "smart_routing/common_vrp_db.py",
    "smart_routing/common_vrp_runtime.py",
    "smart_routing/data_catalog.py",
    "smart_routing/geocode_storage.py",
    "smart_routing/google_geocoder.py",
    "smart_routing/here_geocoder.py",
    "smart_routing/live_atlanta_runtime.py",
    "smart_routing/nominatim_geocoder.py",
    "smart_routing/osrm_routing.py",
    "smart_routing/production_atlanta.py",
    "smart_routing/region_design.py",
    "smart_routing/region_sweep.py",
    "smart_routing/routing_compare.py",
    "smart_routing/service_preprocess.py",
    "smart_routing/us_geocode_cleaner.py",
    "smart_routing/vrp_api_common.py",
    "smart_routing/vrp_api_server.py",
    "smart_routing/vrp_api_service.py",
    "smart_routing/vrp_mode_na_general.py",
    "smart_routing/vrp_mode_z_weekend.py",
    "smart_routing/production_assign_atlanta.py",
    "smart_routing/production_assign_atlanta_vrp.py"
)
foreach ($relativePath in $SmartRoutingRuntimeFiles) {
    Copy-RequiredFile -SourceRelativePath $relativePath -OutputRelativePath $relativePath
}

# services is an operational adapter layer. Only the request-serving API subset
# belongs in the server runtime artifact; DB admin, build, and test tools stay local.
$ServiceRuntimeFiles = @(
    "services/__init__.py",
    "services/api/__init__.py",
    "services/api/common_vrp_config.py",
    "services/api/run_common_vrp_api.py",
    "services/api/sr_vrp_api_server.py"
)
foreach ($relativePath in $ServiceRuntimeFiles) {
    Copy-RequiredFile -SourceRelativePath $relativePath -OutputRelativePath $relativePath
}

$SystemdUnitFiles = if ($IsProduction) {
    @("systemd/common-vrp.service", "systemd/common-vrp-client.service", "systemd/smart-routing.service")
} else {
    @("systemd/common-vrp-dev.service", "systemd/common-vrp-client-dev.service", "systemd/smart-routing-dev.service")
}
foreach ($relativePath in $SystemdUnitFiles) {
    Copy-RequiredFile -SourceRelativePath $relativePath -OutputRelativePath $relativePath
}

if ($IncludeDocs) {
    Copy-DirectoryFiltered -DirectoryName "docs" -AllowedExtensions @(".md", ".txt", ".docx")
    Copy-RequiredFile -SourceRelativePath "services/README.md" -OutputRelativePath "services/README.md"
}

Copy-RequiredFile -SourceRelativePath "config/config.template.json" -OutputRelativePath "config/config.template.json"
$EnvironmentConfigTemplate = if ($IsProduction) {
    "config/common_vrp.prod.template.json"
} else {
    "config/common_vrp.dev.template.json"
}
Copy-RequiredFile -SourceRelativePath $EnvironmentConfigTemplate -OutputRelativePath $EnvironmentConfigTemplate
Copy-RequiredFile -SourceRelativePath "config/data_catalog.json" -OutputRelativePath "config/data_catalog.json"
Copy-RequiredFile -SourceRelativePath "services/deploy/requirements.txt" -OutputRelativePath "requirements.txt"

$RequiredArtifactPaths = @(
    "requirements.txt",
    "config/config.template.json",
    "config/data_catalog.json"
) + @($RuntimeRootFiles) + @($SmartRoutingRuntimeFiles) + @($ServiceRuntimeFiles) + @($SystemdUnitFiles) + @($EnvironmentConfigTemplate)
foreach ($relativePath in $RequiredArtifactPaths) {
    if (-not (Test-Path -LiteralPath (Join-Path $StagingDir $relativePath) -PathType Leaf)) {
        throw "Deploy artifact is incomplete; missing $relativePath"
    }
}

$ArtifactFiles = Get-ChildItem -LiteralPath $StagingDir -Recurse -File | ForEach-Object {
    $relativePath = (Get-RelativePathFromBase -BasePath $StagingDir -FullName $_.FullName).Replace("\", "/")
    [ordered]@{
        path = $relativePath
        sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    }
}

$SourceMode = if ($IsProduction) { "immutable-git-archive" } else { "worktree" }
$Manifest = [ordered]@{
    package_name = $PackageName
    artifact_type = "server-runtime"
    created_at = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.ffffff'Z'", [System.Globalization.CultureInfo]::InvariantCulture)
    source_revision = $SourceRevision
    source_dirty = [bool]$SourceDirty
    source_mode = $SourceMode
    promotable = [bool]($IsProduction -and -not $SourceDirty)
    target_environment = $Environment
    target_root = "/home/csda/AI_Routing/$Environment"
    enabled_services = @("common_api", "common_ui", "smart_routing_api")
    runtime_scope = @("smart_routing", "services/api", "root_entrypoints", "matching_systemd_units")
    includes_docs = [bool]$IncludeDocs
    files = @($ArtifactFiles)
    notes = @(
        "Environment JSON files with values are not included; only checked-in templates are packaged.",
        "Production release artifacts must have source_dirty=false and cannot use -AllowDirtySource.",
        "Copy the checked-in templates to environment JSON files and fill secrets on the target server.",
        "Create production server-only configuration from templates. Development secure configuration is uploaded separately by the deployment console.",
        "Runtime data, logs, caches, OSRM graph files, job results, and local data folders are always excluded.",
        "DB admin/seed tools, deploy builders, tests, offline tools, OSRM operations, and non-server root applications are excluded.",
        "Run with -IncludeDocs to include documentation. Supply runtime data separately through the hydration process."
    )
}
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText(
    (Join-Path $StagingDir "deploy_manifest.json"),
    ($Manifest | ConvertTo-Json -Depth 20),
    $Utf8NoBom
)

Compress-Archive -Path (Join-Path $StagingDir "*") -DestinationPath $ZipPath -CompressionLevel Optimal
if (-not (Test-Path -LiteralPath $ZipPath -PathType Leaf)) {
    throw "Deploy package archive was not created."
}
$BuiltZipSha256 = Get-FileSha256WithRetry -Path $ZipPath

# Re-check immediately before publication. File/Directory.Move do not overwrite
# and are atomic because the build session is under the same environment root.
if (Test-Path $FinalStagingDir) {
    throw "Artifact staging appeared during build; refusing to publish: $FinalStagingDir"
}
if (Test-Path $FinalZipPath) {
    throw "Artifact archive appeared during build; refusing to publish: $FinalZipPath"
}
Move-PublishPathWithRetry -Kind "File" -SourcePath $ZipPath -DestinationPath $FinalZipPath
try {
    Move-PublishPathWithRetry -Kind "Directory" -SourcePath $StagingDir -DestinationPath $FinalStagingDir
}
catch {
    if (
        (Test-Path -LiteralPath $FinalZipPath -PathType Leaf) -and
        (Get-FileSha256WithRetry -Path $FinalZipPath) -eq $BuiltZipSha256
    ) {
        Remove-Item -LiteralPath $FinalZipPath -Force
    }
    throw
}
Write-Host "Deploy package created: $FinalZipPath"
}
finally {
    try {
        if ($null -ne $BuildSessionDir -and (Test-Path -LiteralPath $BuildSessionDir)) {
            $ResolvedBuildingRoot = [System.IO.Path]::GetFullPath((Join-Path $EnvironmentOutputRoot "_building")).TrimEnd([System.IO.Path]::DirectorySeparatorChar)
            $ResolvedBuildSessionDir = [System.IO.Path]::GetFullPath($BuildSessionDir)
            $BuildSessionLeaf = Split-Path $ResolvedBuildSessionDir -Leaf
            $ExpectedBuildingPrefix = "$ResolvedBuildingRoot$([System.IO.Path]::DirectorySeparatorChar)"
            if (
                -not $ResolvedBuildSessionDir.StartsWith($ExpectedBuildingPrefix, [System.StringComparison]::OrdinalIgnoreCase) -or
                $BuildSessionLeaf -notmatch "^runtime-[0-9a-f]{32}$"
            ) {
                throw "Refusing to remove an unsafe build session path: $ResolvedBuildSessionDir"
            }
            Remove-Item -LiteralPath $ResolvedBuildSessionDir -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
    finally {
        if ($null -ne $SnapshotTempDir -and (Test-Path -LiteralPath $SnapshotTempDir)) {
            $ResolvedTempRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath()).TrimEnd([System.IO.Path]::DirectorySeparatorChar)
            $ResolvedSnapshotTempDir = [System.IO.Path]::GetFullPath($SnapshotTempDir)
            $SnapshotLeaf = Split-Path $ResolvedSnapshotTempDir -Leaf
            $ExpectedTempPrefix = "$ResolvedTempRoot$([System.IO.Path]::DirectorySeparatorChar)"
            if (
                -not $ResolvedSnapshotTempDir.StartsWith($ExpectedTempPrefix, [System.StringComparison]::OrdinalIgnoreCase) -or
                $SnapshotLeaf -notmatch "^ai-routing-runtime-snapshot-[0-9a-f]{32}$"
            ) {
                throw "Refusing to remove an unsafe production snapshot path: $ResolvedSnapshotTempDir"
            }
            Remove-Item -LiteralPath $ResolvedSnapshotTempDir -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
}
