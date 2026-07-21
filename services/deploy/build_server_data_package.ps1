param(
    [Parameter(Mandatory = $true)][ValidatePattern("^[0-9A-Za-z._-]+$")][string]$Version,
    [string]$CatalogPath = "config/data_catalog.json",
    [string]$OutputDir = "deployment/server_data",
    [switch]$AcknowledgeSensitiveData,
    [switch]$SkipArchive
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
    -Path (Join-Path -Path $PSScriptRoot -ChildPath "../..") `
    -MustExist
if (-not $AcknowledgeSensitiveData) {
    throw "Server data contains restricted service/profile/technician data. Re-run with -AcknowledgeSensitiveData after confirming the target server and access controls."
}
$CatalogFile = if ([IO.Path]::IsPathRooted($CatalogPath)) { $CatalogPath } else { Join-Path $Root $CatalogPath }
if (-not (Test-Path -LiteralPath $CatalogFile -PathType Leaf)) {
    throw "Data catalog is missing: $CatalogFile"
}

$Catalog = Get-Content -LiteralPath $CatalogFile -Raw -Encoding UTF8 | ConvertFrom-Json
if ($Catalog.schema -ne "north-america-routing-data-catalog/v1") {
    throw "Unsupported data catalog schema: $($Catalog.schema)"
}
$DataRoot = if ([IO.Path]::IsPathRooted([string]$Catalog.data_root)) {
    [IO.Path]::GetFullPath([string]$Catalog.data_root)
} else {
    [IO.Path]::GetFullPath((Join-Path $Root ([string]$Catalog.data_root)))
}
$DataRoot = [IO.Path]::GetFullPath($DataRoot)
$OutputRoot = if ([IO.Path]::IsPathRooted($OutputDir)) { $OutputDir } else { Join-Path $Root $OutputDir }
$OutputRoot = [IO.Path]::GetFullPath($OutputRoot)
$AllowedOutputRoot = [IO.Path]::GetFullPath((Join-Path $Root "deployment/server_data"))
if ($OutputRoot -ne $AllowedOutputRoot -and -not $OutputRoot.StartsWith($AllowedOutputRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Server data output must stay under $AllowedOutputRoot`: $OutputRoot"
}
$PackageRoot = [IO.Path]::GetFullPath((Join-Path $OutputRoot $Version))
if (-not $PackageRoot.StartsWith($OutputRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Package output escapes the requested output directory: $PackageRoot"
}
$SharedRoot = Join-Path $PackageRoot "shared"
$SharedDataRoot = Join-Path $SharedRoot "north_america"

if (Test-Path -LiteralPath $PackageRoot) {
    Remove-Item -LiteralPath $PackageRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $SharedDataRoot -Force | Out-Null

$FileRoles = @(
    "service_geocoded",
    "profile_production",
    "client_master",
    "zcta_geometry",
    "symptom_mapping",
    "heavy_repair_lookup",
    "technician_map",
    "atlanta_engineer_region",
    "atlanta_engineer_home"
)
$DirectoryRoles = @("reviewed_regions_dir", "region_seed_dir")
$VirtualStateRoles = @("region_candidates_dir", "reports_dir")
$ManifestFiles = New-Object System.Collections.Generic.List[object]
$RestrictedRoles = @(
    "service_geocoded",
    "profile_production",
    "technician_map",
    "atlanta_engineer_region",
    "atlanta_engineer_home"
)

function Get-RoleValue([string]$Role) {
    $Property = $Catalog.active.PSObject.Properties[$Role]
    if ($null -eq $Property -or [string]::IsNullOrWhiteSpace([string]$Property.Value)) {
        throw "Required server data role is missing from catalog: $Role"
    }
    return [string]$Property.Value
}

function Copy-ServerDataFile([string]$SourcePath, [string]$RelativePath, [string]$Role) {
    $SourcePath = [IO.Path]::GetFullPath($SourcePath)
    if (-not $SourcePath.StartsWith($DataRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Catalog role escapes data_root ($Role): $SourcePath"
    }
    if (-not (Test-Path -LiteralPath $SourcePath -PathType Leaf)) {
        throw "Required server data is missing ($Role): $SourcePath"
    }
    $Normalized = $RelativePath.Replace("\", "/").TrimStart("/")
    if ($Normalized -match '(^|/)(raw/service|runtime|reports|planning|catalog)(/|$)') {
        throw "Server data package cannot contain local-only lifecycle data: $Normalized"
    }
    $Target = [IO.Path]::GetFullPath((Join-Path $SharedDataRoot $Normalized))
    if (-not $Target.StartsWith($SharedDataRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Package target escapes shared data root ($Role): $Target"
    }
    New-Item -ItemType Directory -Path (Split-Path $Target -Parent) -Force | Out-Null
    Copy-Item -LiteralPath $SourcePath -Destination $Target -Force
    $SourceHash = (Get-FileHash -LiteralPath $SourcePath -Algorithm SHA256).Hash.ToLowerInvariant()
    $TargetHash = (Get-FileHash -LiteralPath $Target -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($SourceHash -ne $TargetHash) {
        throw "Checksum mismatch after copying $Role to $Target"
    }
    $ManifestFiles.Add([ordered]@{
        role = $Role
        path = "shared/north_america/$Normalized"
        size_bytes = (Get-Item -LiteralPath $Target).Length
        sha256 = $TargetHash
        classification = if ($RestrictedRoles -contains $Role) { "restricted-personal-data" } else { "internal" }
    })
}

foreach ($Role in $FileRoles) {
    $Relative = Get-RoleValue $Role
    if ([IO.Path]::IsPathRooted($Relative)) {
        throw "Local package catalog roles must be relative to data_root ($Role): $Relative"
    }
    Copy-ServerDataFile (Join-Path $DataRoot $Relative) $Relative $Role
}
foreach ($Role in $DirectoryRoles) {
    $Relative = Get-RoleValue $Role
    $SourceDir = Join-Path $DataRoot $Relative
    if (-not (Test-Path -LiteralPath $SourceDir -PathType Container)) {
        throw "Required server data directory is missing ($Role): $SourceDir"
    }
    $Files = @(Get-ChildItem -LiteralPath $SourceDir -Recurse -File)
    if ($Files.Count -eq 0) {
        throw "Required server data directory is empty ($Role): $SourceDir"
    }
    foreach ($File in $Files) {
        $Child = $File.FullName.Substring($SourceDir.Length).TrimStart('\', '/')
        Copy-ServerDataFile $File.FullName (Join-Path $Relative $Child) $Role
    }
}

$ServerActive = [ordered]@{}
foreach ($Role in @($FileRoles + $DirectoryRoles + $VirtualStateRoles)) {
    $ServerActive[$Role] = Get-RoleValue $Role
}
# Runtime roles intentionally point at minimized processed artifacts. Raw source
# roles are omitted from the server catalog and package.
$ServerActive["profile_runtime"] = Get-RoleValue "profile_production"
$SharedConfigRoot = Join-Path $SharedRoot "config"
New-Item -ItemType Directory -Path $SharedConfigRoot -Force | Out-Null
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
foreach ($Environment in @("development", "production")) {
    $ServerCatalog = [ordered]@{
        schema = "north-america-routing-data-catalog/v1"
        data_root = "/home/csda/AI_Routing/shared/north_america"
        state_root = "/home/csda/AI_Routing/state/$Environment"
        active = $ServerActive
        region_plans = $Catalog.region_plans
    }
    [IO.File]::WriteAllText(
        (Join-Path $SharedConfigRoot "data_catalog.$Environment.json"),
        ($ServerCatalog | ConvertTo-Json -Depth 20),
        $Utf8NoBom
    )
    if ($Environment -eq "production") {
        [IO.File]::WriteAllText(
            (Join-Path $SharedConfigRoot "data_catalog.json"),
            ($ServerCatalog | ConvertTo-Json -Depth 20),
            $Utf8NoBom
        )
    }
}
foreach ($CatalogName in @("data_catalog.development.json", "data_catalog.production.json", "data_catalog.json")) {
    $CatalogArtifact = Join-Path $SharedConfigRoot $CatalogName
    $ManifestFiles.Add([ordered]@{
        role = "server_catalog"
        path = "shared/config/$CatalogName"
        size_bytes = (Get-Item -LiteralPath $CatalogArtifact).Length
        sha256 = (Get-FileHash -LiteralPath $CatalogArtifact -Algorithm SHA256).Hash.ToLowerInvariant()
        classification = "internal"
    })
}

foreach ($Environment in @("development", "production")) {
    foreach ($StateDir in @("common_vrp_jobs", "vrp_api_jobs", "cache", "logs", "reports", "planning/regions/candidates")) {
        $Directory = Join-Path $PackageRoot "state/$Environment/$StateDir"
        New-Item -ItemType Directory -Path $Directory -Force | Out-Null
        "Runtime-owned directory. Do not share between development and production." |
            Set-Content -LiteralPath (Join-Path $Directory "README.txt") -Encoding UTF8
    }
}

$Manifest = [ordered]@{
    schema = "ai-routing-server-data-package/v1"
    version = $Version
    created_at = (Get-Date).ToUniversalTime().ToString("o")
    target_root = "/home/csda/AI_Routing"
    shared_data_root = "/home/csda/AI_Routing/shared/north_america"
    contains_restricted_personal_data = $true
    sensitive_data_acknowledged = [bool]$AcknowledgeSensitiveData
    catalog_paths = @(
        "/home/csda/AI_Routing/shared/config/data_catalog.development.json",
        "/home/csda/AI_Routing/shared/config/data_catalog.production.json"
    )
    local_only_excluded = @("raw", "planning", "reports", "runtime", "cache", "260310")
    files = $ManifestFiles
}
[IO.File]::WriteAllText(
    (Join-Path $PackageRoot "manifest.json"),
    ($Manifest | ConvertTo-Json -Depth 20),
    $Utf8NoBom
)

$ArchivePath = Join-Path $OutputRoot "ai-routing-server-data-$Version.zip"
if (-not $SkipArchive) {
    if (Test-Path -LiteralPath $ArchivePath) { Remove-Item -LiteralPath $ArchivePath -Force }
    Compress-Archive -Path (Join-Path $PackageRoot "*") -DestinationPath $ArchivePath -CompressionLevel Optimal
}

[ordered]@{
    package_root = $PackageRoot
    archive = if ($SkipArchive) { $null } else { $ArchivePath }
    file_count = $ManifestFiles.Count
    target_root = "/home/csda/AI_Routing"
} | ConvertTo-Json -Depth 5
