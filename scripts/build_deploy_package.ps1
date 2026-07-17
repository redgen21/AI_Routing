param(
    [string]$Version = "",
    [string]$OutputDir = "dist",
    [switch]$IncludeDocs,
    [switch]$IncludeRuntimeData
)

$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
if ([string]::IsNullOrWhiteSpace($Version)) {
    $Version = Get-Date -Format "yyyyMMdd-HHmmss"
}

$PackageName = "ai-routing-deploy-$Version"
$OutputRoot = Join-Path $Root $OutputDir
$StagingDir = Join-Path $OutputRoot $PackageName
$ZipPath = Join-Path $OutputRoot "$PackageName.zip"

if (Test-Path $StagingDir) {
    Remove-Item -LiteralPath $StagingDir -Recurse -Force
}
if (Test-Path $ZipPath) {
    Remove-Item -LiteralPath $ZipPath -Force
}
New-Item -ItemType Directory -Path $StagingDir -Force | Out-Null

function Get-PackageRelativePath {
    param([Parameter(Mandatory = $true)][string]$FullName)

    $rootPath = [System.IO.Path]::GetFullPath("$Root")
    if (-not $rootPath.EndsWith([System.IO.Path]::DirectorySeparatorChar)) {
        $rootPath += [System.IO.Path]::DirectorySeparatorChar
    }
    $filePath = [System.IO.Path]::GetFullPath($FullName)
    $rootUri = New-Object System.Uri($rootPath)
    $fileUri = New-Object System.Uri($filePath)
    return [System.Uri]::UnescapeDataString($rootUri.MakeRelativeUri($fileUri).ToString()).Replace("/", [System.IO.Path]::DirectorySeparatorChar)
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

function Test-ExcludedFile {
    param([string]$RelativePath)

    $normalized = $RelativePath -replace "\\", "/"
    $name = Split-Path $RelativePath -Leaf

    if ($normalized -match "(^|/)(__pycache__|\.git|\.pytest_cache|\.streamlit|dist|log)(/|$)") { return $true }
    if ($normalized -match "(^|/)(260310|260507|260514|data)(/|$)" -and -not $IncludeRuntimeData) { return $true }
    if ($normalized -match "\.(pyc|pyo|pyd|log|tmp|bak)$") { return $true }
    if ($normalized -match "\.(csv|xlsx|xls|parquet)$" -and -not $IncludeRuntimeData) { return $true }
    if ($normalized -match "\.(osm|osm\.pbf|osrm|osrm\.fileIndex|osrm\.geometry|osrm\.hsgr|osrm\.mldgr|osrm\.names|osrm\.properties|osrm\.ramIndex|osrm\.timestamp|osrm\.turn_duration_penalties|osrm\.turn_penalties|osrm\.datasource_names)$") { return $true }
    if ($name -like "~$*") { return $true }
    return $false
}

function Copy-DirectoryFiltered {
    param(
        [Parameter(Mandatory = $true)][string]$DirectoryName,
        [string[]]$AllowedExtensions = @()
    )

    $SourceDir = Join-Path $Root $DirectoryName
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

function ConvertTo-SanitizedObject {
    param([object]$Value)

    if ($null -eq $Value) {
        return $null
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string] -and $Value -isnot [pscustomobject]) {
        $items = @()
        foreach ($item in $Value) {
            $items += ConvertTo-SanitizedObject $item
        }
        return $items
    }

    if ($Value -is [pscustomobject]) {
        $result = [ordered]@{}
        foreach ($property in $Value.PSObject.Properties) {
            if ($property.Name -match "(?i)(password|secret|token|api[_-]?key|credential|private[_-]?key)") {
                $result[$property.Name] = "<REPLACE_ME>"
            } else {
                $result[$property.Name] = ConvertTo-SanitizedObject $property.Value
            }
        }
        return [pscustomobject]$result
    }

    return $Value
}

function Add-SanitizedConfigTemplate {
    param([string]$ConfigName)

    $SourcePath = Join-Path $Root $ConfigName
    if (-not (Test-Path $SourcePath)) {
        return
    }

    $templateName = [System.IO.Path]::GetFileNameWithoutExtension($ConfigName) + ".template.json"
    $TargetPath = Join-Path $StagingDir $templateName
    $json = Get-Content -LiteralPath $SourcePath -Raw | ConvertFrom-Json -Depth 100
    $sanitized = ConvertTo-SanitizedObject $json
    $sanitized | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $TargetPath -Encoding UTF8
}

$RootFilePatterns = @("*.py", "*.sh", "*.ps1", "*.bat", "README.md", "UPDATED_BY_CODEX.md")
foreach ($pattern in $RootFilePatterns) {
    Get-ChildItem -LiteralPath $Root -File -Filter $pattern | ForEach-Object {
        if ($_.Name -match "^config.*\.json$") {
            return
        }
        $relative = Get-PackageRelativePath $_.FullName
        if (-not (Test-ExcludedFile $relative)) {
            Copy-FileToPackage -SourcePath $_.FullName -RelativePath $relative
        }
    }
}

Copy-DirectoryFiltered -DirectoryName "smart_routing" -AllowedExtensions @(".py", ".json", ".md", ".txt", ".sql")
Copy-DirectoryFiltered -DirectoryName "systemd" -AllowedExtensions @(".service", ".md", ".txt")
Copy-DirectoryFiltered -DirectoryName "osrm" -AllowedExtensions @(".sh", ".md", ".txt", ".lua")

if ($IncludeDocs) {
    Copy-DirectoryFiltered -DirectoryName "docs" -AllowedExtensions @(".md", ".txt", ".docx")
}

Add-SanitizedConfigTemplate -ConfigName "config.json"
Add-SanitizedConfigTemplate -ConfigName "config_common_vrp.json"

$Manifest = [ordered]@{
    package_name = $PackageName
    created_at = (Get-Date).ToString("s")
    source_root = "$Root"
    includes_docs = [bool]$IncludeDocs
    includes_runtime_data = [bool]$IncludeRuntimeData
    notes = @(
        "Original config.json and config_common_vrp.json are not included.",
        "Use config.template.json and config_common_vrp.template.json, then fill secrets on the target server.",
        "Runtime data, logs, caches, OSRM graph files, job results, and local data folders are excluded by default.",
        "Run with -IncludeDocs to include docs, and -IncludeRuntimeData only for an internal backup package."
    )
}
$Manifest | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $StagingDir "deploy_manifest.json") -Encoding UTF8

Compress-Archive -LiteralPath (Join-Path $StagingDir "*") -DestinationPath $ZipPath -CompressionLevel Optimal
Write-Host "Deploy package created: $ZipPath"
