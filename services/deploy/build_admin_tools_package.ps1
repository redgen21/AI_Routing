param(
    [string]$Version = "",
    [string]$OutputDir = "deployment",
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

function Resolve-VerifiedPythonExecutable {
    $command = @(Get-Command python -CommandType Application -ErrorAction Stop | Select-Object -First 1)[0]
    $candidate = [IO.Path]::GetFullPath([string]$command.Source)
    if (
        [string]::IsNullOrWhiteSpace($candidate) -or
        -not [IO.Path]::IsPathRooted($candidate) -or
        -not (Test-Path -LiteralPath $candidate -PathType Leaf)
    ) {
        throw "A filesystem Python executable is required for the admin-tools import smoke."
    }
    $versionOutput = @(& $candidate "-c" "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>&1)
    if ($LASTEXITCODE -ne 0 -or $versionOutput.Count -ne 1) {
        throw "Python executable validation failed for the admin-tools import smoke."
    }
    try {
        $version = [Version]([string]$versionOutput[0]).Trim()
    }
    catch {
        throw "Python executable returned an invalid version for the admin-tools import smoke."
    }
    if ($version -lt [Version]"3.10") {
        throw "Admin-tools import smoke requires Python 3.10 or later."
    }
    return $candidate
}

function Invoke-AdminToolsImportSmoke {
    param(
        [Parameter(Mandatory = $true)][string]$PythonExecutable,
        [Parameter(Mandatory = $true)][string]$StagingDirectory
    )

    # -I ignores PYTHONPATH, user site-packages, and the caller's CWD.  The
    # smoke script inserts only the generated staging directory before loading
    # every packaged executable entrypoint and its release-local dependencies.
$smokeScript = @'
import importlib
import sys
from pathlib import Path

# This temporary file lives in staging, avoiding native-shell quote handling
# for a multi-line ``-c`` script. It is deleted before manifest creation.
staging = Path(__file__).resolve().parent
sys.path.insert(0, str(staging))
module_names = (
    "admin_tools.db.master_data_backend",
    "admin_tools.db.common_vrp",
    "admin_tools.db.data_catalog",
    "admin_tools.db.heavy_repair",
    "admin_tools.db.runners.reset_common_vrp_data",
    "admin_tools.db.runners.upsert_profile_capabilities",
    "admin_tools.db.seeds.build_la_bucket_vrp_inputs",
    "admin_tools.db.seeds.import_asia_technician_centroids",
    "admin_tools.db.migration_runner",
)
candidate_backend = staging / "admin_tools" / "db" / "region_plan_backend.py"
if candidate_backend.is_file():
    module_names += ("admin_tools.db.region_plan_backend",)
technician_profile_backend = staging / "admin_tools" / "db" / "technician_profile_backend.py"
if technician_profile_backend.is_file():
    module_names += ("admin_tools.db.technician_profile_backend",)
region_plan_schema_backend = staging / "admin_tools" / "db" / "region_plan_schema_backend.py"
if region_plan_schema_backend.is_file():
    module_names += ("admin_tools.db.region_plan_schema_backend",)
region_plan_v2_backend = staging / "admin_tools" / "db" / "region_plan_v2_backend.py"
if region_plan_v2_backend.is_file():
    module_names += ("admin_tools.db.region_plan_v2_backend",)
modules = tuple(importlib.import_module(name) for name in module_names)
for module in modules:
    location = Path(module.__file__).resolve()
    if not location.is_relative_to(staging):
        raise RuntimeError(f"package import escaped staging: {module.__name__}")
backend = modules[0]
if backend.CONTRACT_VERSION != "db-admin/v1" or len(backend.TABLE_REGISTRY) != 13:
    raise RuntimeError("packaged master-data contract is invalid")
'@
    $smokePath = Join-Path $StagingDirectory ".admin_tools_import_smoke.py"
    [IO.File]::WriteAllText($smokePath, $smokeScript, [Text.UTF8Encoding]::new($false))
    $previousErrorActionPreference = $ErrorActionPreference
    Push-Location -LiteralPath $StagingDirectory
    try {
        # A dependency warning on stderr must not bypass the explicit exit-code
        # gate below or leak into build output. On the Windows/NAS development
        # workspace, endpoint scanning can transiently deny Python a newly
        # copied module. Retry in a fresh isolated interpreter; deterministic
        # syntax/import failures still fail every attempt.
        $ErrorActionPreference = "Continue"
        $smokeExitCode = 1
        for ($smokeAttempt = 1; $smokeAttempt -le 3; $smokeAttempt++) {
            $smokeOutput = @(& $PythonExecutable "-I" "-B" $smokePath 2>&1)
            $smokeExitCode = $LASTEXITCODE
            if ($smokeExitCode -eq 0) {
                break
            }
            if ($smokeAttempt -lt 3) {
                Start-Sleep -Milliseconds 250
            }
        }
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
        Pop-Location
        Remove-Item -LiteralPath $smokePath -Force -ErrorAction SilentlyContinue
    }
    if ($smokeExitCode -ne 0) {
        # Do not expose import output: a future dependency could include local
        # filesystem or configuration context in its exception text.
        throw "Admin-tools staging import smoke failed."
    }
    if (@(Get-ChildItem -LiteralPath $StagingDirectory -Recurse -Directory -Filter "__pycache__").Count -ne 0) {
        throw "Admin-tools staging import smoke created forbidden bytecode caches."
    }
}

$Root = ConvertTo-NativeFileSystemPath `
    -Path (Join-Path -Path $PSScriptRoot -ChildPath "..\..") `
    -MustExist
$SourceRevision = [string](& git -C $Root rev-parse HEAD)
$RevisionExitCode = $LASTEXITCODE
$SourceRevision = $SourceRevision.Trim()
if ($RevisionExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($SourceRevision)) {
    throw "A Git checkout with a resolvable HEAD is required to build admin tools."
}
$SourceStatus = @(& git -C $Root status --porcelain --untracked-files=all)
if ($LASTEXITCODE -ne 0) {
    throw "Unable to inspect Git source status; refusing to build."
}
$SourceDirty = $SourceStatus.Count -gt 0
if ($SourceDirty -and -not $AllowDirtySource) {
    throw "Admin tools require a clean checkout. Use -AllowDirtySource only for development verification."
}
$PythonExecutable = Resolve-VerifiedPythonExecutable
if ([string]::IsNullOrWhiteSpace($Version)) {
    $Version = Get-Date -Format "yyyyMMdd-HHmmss"
}
if ($Version -notmatch "^[A-Za-z0-9][A-Za-z0-9._-]*$") {
    throw "Version may contain only letters, numbers, dot, underscore, and hyphen."
}

$PackageName = "ai-routing-admin-tools-$Version"
$RootPath = [IO.Path]::GetFullPath("$Root").TrimEnd([IO.Path]::DirectorySeparatorChar)
$OutputRoot = [IO.Path]::GetFullPath((Join-Path $Root $OutputDir))
$AdminOutputRoot = Join-Path $OutputRoot "admin_tools"
$StagingDir = Join-Path $AdminOutputRoot $Version
$ZipPath = Join-Path $AdminOutputRoot "$PackageName.zip"
if (-not $OutputRoot.StartsWith("$RootPath$([IO.Path]::DirectorySeparatorChar)", [StringComparison]::OrdinalIgnoreCase)) {
    throw "OutputDir must resolve inside the repository root: $OutputRoot"
}

if ((Test-Path -LiteralPath $StagingDir) -or (Test-Path -LiteralPath $ZipPath)) {
    throw "Admin-tools artifact output already exists; choose a new version."
}
New-Item -ItemType Directory -Path $StagingDir -Force | Out-Null

function Copy-FileToPackage {
    param(
        [Parameter(Mandatory = $true)][string]$SourcePath,
        [Parameter(Mandatory = $true)][string]$RelativePath
    )
    $target = Join-Path $StagingDir $RelativePath
    $parent = Split-Path $target -Parent
    if (-not (Test-Path -LiteralPath $parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    Copy-Item -LiteralPath $SourcePath -Destination $target -Force
}

function Copy-RequiredFile {
    param([Parameter(Mandatory = $true)][string]$RelativePath)
    $source = Join-Path $Root $RelativePath
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Required admin-tools source is missing: $RelativePath"
    }
    Copy-FileToPackage -SourcePath $source -RelativePath $RelativePath
}

$PackageSourcePaths = @(
    "admin_tools/__init__.py",
    "admin_tools/db/__init__.py",
    "admin_tools/db/guard.py",
    "admin_tools/db/master_data_backend.py",
    "admin_tools/db/release_backend.py",
    "admin_tools/db/migration_runner.py",
    "admin_tools/db/common_vrp.py",
    "admin_tools/db/data_catalog.py",
    "admin_tools/db/heavy_repair.py",
    "admin_tools/db/README.md",
    "admin_tools/db/migrations/__init__.py",
    "admin_tools/db/migrations/README.md",
    "admin_tools/db/migrations/manifest.json",
    "admin_tools/db/migrations/V001__atlanta_6area_region_plan.manifest.json",
    "admin_tools/db/migrations/V001__atlanta_6area_region_plan.sql",
    "admin_tools/db/migrations/V002__region_plan_unbounded_region_seq.sql",
    "admin_tools/db/migrations/V003__region_plan_technician_source_id.sql",
    "admin_tools/db/migrations/V004__region_plan_area_type_region_soft.sql",
    "admin_tools/db/migrations/V005__area_plan_catalog.sql",
    "admin_tools/db/region_plan_schema_backend.py",
    "admin_tools/db/region_plan_schema_v2.sql",
    "admin_tools/db/region_plan_v2_backend.py",
    "admin_tools/db/runners/__init__.py",
    "admin_tools/db/runners/reset_common_vrp_data.py",
    "admin_tools/db/runners/upsert_profile_capabilities.py",
    "admin_tools/db/seeds/__init__.py",
    "admin_tools/db/seeds/build_la_bucket_vrp_inputs.py",
    "admin_tools/db/seeds/import_asia_technician_centroids.py",
    "config/config.template.json",
    "config/common_vrp.dev.template.json",
    "config/common_vrp.prod.template.json",
    "config/data_catalog.admin.template.json",
    "config/data_catalog.json"
)
# Candidate-plan staging is an optional, fixed Admin Tools capability.  The
# package never reaches back into the checkout: if the data-owned CLI is not
# present, it is absent from both the exact allowlist and the manifest, and
# the console bridge refuses to stage territory candidates through that
# release.  Once present it is imported from the generated staging directory.
$RegionPlanBackend = "admin_tools/db/region_plan_backend.py"
if (Test-Path -LiteralPath (Join-Path $Root $RegionPlanBackend) -PathType Leaf) {
    # This CLI deliberately uses the data-owned immutable workbook parser.
    # Package only those exact Python package files so the isolated staging
    # smoke cannot fall back to the source checkout.
    $RegionPlanSupportPaths = @(
        "tools/__init__.py",
        "tools/data/__init__.py",
        "tools/data/atlanta_6area_plan.py"
    )
    $PackageSourcePaths += @($RegionPlanBackend) + $RegionPlanSupportPaths
}
# Technician profile synchronization is likewise opt-in at build time, but it
# has a fixed executable and transform dependency.  If either source is
# missing it is absent from the exact manifest and the console rejects that
# pinned release; no import may escape the generated staging directory.
$TechnicianProfileBackend = "admin_tools/db/technician_profile_backend.py"
if (Test-Path -LiteralPath (Join-Path $Root $TechnicianProfileBackend) -PathType Leaf) {
    $TechnicianProfileSupportPaths = @(
        "tools/__init__.py",
        "tools/data/__init__.py",
        "tools/data/technician_profile_data.py"
    )
    $PackageSourcePaths += @($TechnicianProfileBackend) + $TechnicianProfileSupportPaths
}
$PackageSourcePaths = @($PackageSourcePaths | Select-Object -Unique)
foreach ($relativePath in $PackageSourcePaths) {
    Copy-RequiredFile -RelativePath $relativePath
}
Copy-FileToPackage `
    -SourcePath (Join-Path $Root "services/deploy/requirements.txt") `
    -RelativePath "requirements.txt"

$RequiredPaths = @($PackageSourcePaths) + @("requirements.txt")
foreach ($relativePath in $RequiredPaths) {
    if (-not (Test-Path -LiteralPath (Join-Path $StagingDir $relativePath) -PathType Leaf)) {
        throw "Admin-tools artifact is incomplete; missing $relativePath"
    }
}
$ExpectedPaths = @($RequiredPaths | ForEach-Object { $_.Replace("\", "/") } | Sort-Object)
$ActualPaths = @(
    Get-ChildItem -LiteralPath $StagingDir -Recurse -File | ForEach-Object {
        $_.FullName.Substring($StagingDir.Length + 1).Replace("\", "/")
    } | Sort-Object
)
$PathDifference = @(Compare-Object -ReferenceObject $ExpectedPaths -DifferenceObject $ActualPaths)
if ($PathDifference.Count -ne 0) {
    throw "Admin-tools staging does not exactly match the explicit file allowlist."
}

# Fail closed before manifest/ZIP creation.  Admin Tools may contain templates,
# but never resolved credentials or ignored local configuration.
$CredentialAssignment = [regex]::new(
    '(?i)["'']?(?:password|passwd|pwd|secret|token|api[_-]?key)["'']?\s*[:=]\s*["''](?<value>[^"'']*)["'']'
)
$ConnectionCredential = [regex]::new(
    '(?i)[a-z][a-z0-9+.-]*://[^\s:/@]+:[^\s@]+@'
)
$PrivateKeyMarker = [regex]::new(
    '-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'
)
$ScannableExtensions = @(".py", ".json", ".md", ".sql", ".txt")
foreach ($file in Get-ChildItem -LiteralPath $StagingDir -Recurse -File) {
    $relative = $file.FullName.Substring($StagingDir.Length + 1).Replace("\", "/")
    if ($file.Name -match '(?i)\.local\.json$') {
        throw "Admin-tools secret scan rejected local configuration: $relative"
    }
    if ($ScannableExtensions -notcontains $file.Extension.ToLowerInvariant()) {
        continue
    }
    $text = [IO.File]::ReadAllText($file.FullName)
    if ($ConnectionCredential.IsMatch($text) -or $PrivateKeyMarker.IsMatch($text)) {
        throw "Admin-tools secret scan rejected credential material: $relative"
    }
    foreach ($match in $CredentialAssignment.Matches($text)) {
        $value = $match.Groups["value"].Value.Trim()
        $isPlaceholder = (
            [string]::IsNullOrWhiteSpace($value) -or
            $value -match '^<[^>]+>$' -or
            $value -match '^(?i:REPLACE_ME|CHANGE_ME)$'
        )
        if (-not $isPlaceholder) {
            throw "Admin-tools secret scan rejected a resolved credential: $relative"
        }
    }
}

# This must succeed before the manifest declares the package valid and before
# an archive can be produced. A failed smoke leaves only a non-selectable
# staging directory with no deploy_manifest.json.
Invoke-AdminToolsImportSmoke -PythonExecutable $PythonExecutable -StagingDirectory $StagingDir

$Files = Get-ChildItem -LiteralPath $StagingDir -Recurse -File | ForEach-Object {
    [ordered]@{
        path = $_.FullName.Substring($StagingDir.Length + 1).Replace("\", "/")
        sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    }
}
$Entrypoints = @(
    "admin_tools.db.migration_runner",
    "admin_tools.db.runners.reset_common_vrp_data",
    "admin_tools.db.master_data_backend",
    "admin_tools.db.seeds.build_la_bucket_vrp_inputs",
    "admin_tools.db.seeds.import_asia_technician_centroids"
)
if ($PackageSourcePaths -contains $TechnicianProfileBackend) {
    $Entrypoints += "admin_tools.db.technician_profile_backend"
}
# region_plan_backend remains a release-local compatibility dependency for the
# schema reconciler. Do not advertise its historical per-migration CLI as an
# executable release surface.
$Entrypoints += "admin_tools.db.region_plan_schema_backend"
$Entrypoints += "admin_tools.db.region_plan_v2_backend"
$Manifest = [ordered]@{
    package_name = $PackageName
    artifact_type = "db-admin-tools"
    created_at = (Get-Date).ToString("s")
    source_revision = $SourceRevision
    source_dirty = [bool]$SourceDirty
    promotable = [bool](-not $SourceDirty)
    target_root = "/home/csda/AI_Routing/admin_tools"
    contains_secrets = $false
    contains_data = $false
    entrypoints = @($Entrypoints)
    files = @($Files)
    notes = @(
        "No database command is executed by this build.",
        "Dirty-source artifacts are development verification only and are not promotable.",
        "Supply an explicit development or production config path when running a command.",
        "Production writes require a backup, approval, and --confirm-production.",
        "Install as a versioned read-only admin release; do not merge into the application runtime artifact."
    )
}
$ManifestPath = Join-Path $StagingDir "deploy_manifest.json"
$ManifestJson = $Manifest | ConvertTo-Json -Depth 20
[IO.File]::WriteAllText($ManifestPath, $ManifestJson + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))

Compress-Archive -Path (Join-Path $StagingDir "*") -DestinationPath $ZipPath -CompressionLevel Optimal
Write-Host "Admin-tools package created: $ZipPath"
