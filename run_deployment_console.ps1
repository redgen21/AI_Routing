param(
    [string]$SshHost = "20.51.244.68",
    [string]$SshUser = "csda",
    [int]$LocalPort = 18066,
    [int]$RemotePort = 8066
)

$ErrorActionPreference = "Stop"

$sshCommand = Get-Command ssh -ErrorAction Stop
$streamlitCommand = Get-Command streamlit -ErrorAction Stop
$forward = "${LocalPort}:127.0.0.1:${RemotePort}"
$existingTunnel = Get-NetTCPConnection -LocalPort $LocalPort -State Listen -ErrorAction SilentlyContinue
$tunnelProcess = $null
$ownsTunnel = $false

try {
    if (-not $existingTunnel) {
        # The tunnel is local-only; the Region Plan API continues to accept
        # mutations only from its server-side loopback address.
        $tunnelProcess = Start-Process `
            -FilePath $sshCommand.Source `
            -ArgumentList @(
                "-N",
                "-L", $forward,
                "-o", "ExitOnForwardFailure=yes",
                "-o", "BatchMode=yes",
                "$SshUser@$SshHost"
            ) `
            -WindowStyle Hidden `
            -PassThru
        $ownsTunnel = $true
    }

    $ready = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        $connection = Test-NetConnection `
            -ComputerName "127.0.0.1" `
            -Port $LocalPort `
            -InformationLevel Quiet `
            -WarningAction SilentlyContinue
        if ($connection) {
            $ready = $true
            break
        }
        if ($tunnelProcess -and $tunnelProcess.HasExited) {
            throw "SSH tunnel exited before local port $LocalPort became available. Configure SSH key/agent authentication first."
        }
        Start-Sleep -Milliseconds 500
    }
    if (-not $ready) {
        throw "SSH tunnel did not become available on 127.0.0.1:$LocalPort."
    }

    $env:REGION_PLAN_V2_API_ORIGIN = "http://127.0.0.1:$LocalPort"
    & $streamlitCommand.Source run .\sr_deployment_console.py @args
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    if ($ownsTunnel -and $tunnelProcess -and -not $tunnelProcess.HasExited) {
        Stop-Process -Id $tunnelProcess.Id -Force -ErrorAction SilentlyContinue
    }
}
