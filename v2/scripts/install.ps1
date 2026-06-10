# CarbonSync 2.0 daemon installer (Windows).
# Designed to be driven remotely from Carbon Cortex via shell-<box>__exec --
# zero manual PC visits. Node.js must be on PATH (it is on every CortexAgent node).
#
#   .\install.ps1 -Role hub                       # on carbonserver
#   .\install.ps1 -Role spoke -HubUrl https://192.168.0.35:21600 -Token <fleet-token>
#
# Idempotent: re-running updates code + deps, keeps config/state, restarts the task.
param(
  [Parameter(Mandatory = $true)][ValidateSet('hub', 'spoke')][string]$Role,
  [string]$HubUrl,
  [string]$Token,
  [string]$Name,
  # repo's v2/ dir (default: this script's parent dir's parent)
  [string]$SourceDir = ''
)
$ErrorActionPreference = 'Stop'
if (-not $SourceDir) { $SourceDir = Split-Path -Parent $PSScriptRoot }

$installDir = Join-Path $env:ProgramData 'CarbonSync'
$cfgPath = Join-Path $installDir 'config.json'
$taskName = 'CarbonSyncd'
$node = (Get-Command node).Source

Write-Host "[carbonsync] installing to $installDir (source: $SourceDir)"
New-Item -ItemType Directory -Force -Path $installDir | Out-Null
Copy-Item -Recurse -Force (Join-Path $SourceDir 'src') $installDir
Copy-Item -Force (Join-Path $SourceDir 'package.json') $installDir
if (Test-Path (Join-Path $SourceDir 'package-lock.json')) {
  Copy-Item -Force (Join-Path $SourceDir 'package-lock.json') $installDir
}

# Stop the daemon BEFORE npm ci: the loaded native addons (better-sqlite3,
# @parcel/watcher .node files) are locked by Windows while the process runs,
# and npm ci's node_modules wipe hits EPERM. The task restart below revives it.
Write-Host '[carbonsync] stopping daemon for dependency install...'
Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
Get-CimInstance Win32_Process -Filter "Name = 'node.exe'" -ErrorAction SilentlyContinue |
  Where-Object { $_.CommandLine -match 'CarbonSync\\src' } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Start-Sleep -Seconds 1

Push-Location $installDir
try {
  Write-Host '[carbonsync] installing dependencies...'
  # npm.cmd explicitly: PowerShell prefers the npm.ps1 shim under '&', and on
  # some boxes (kingdel) that shim mangles argv ('npm ci' arrives as 'pm').
  if (Test-Path (Join-Path $installDir 'package-lock.json')) { & npm.cmd ci --omit=dev --no-audit --no-fund }
  else { & npm.cmd install --omit=dev --no-audit --no-fund }
  if ($LASTEXITCODE -ne 0) { throw "npm install failed ($LASTEXITCODE)" }

  if (-not (Test-Path $cfgPath)) {
    Write-Host '[carbonsync] generating config...'
    $initArgs = @('src/index.js', 'init', '--config', $cfgPath, '--role', $Role)
    if ($Name) { $initArgs += @('--name', $Name) }
    if ($Token) { $initArgs += @('--token', $Token) }
    if ($Role -eq 'spoke') {
      if (-not $HubUrl) { throw 'spoke install requires -HubUrl (and -Token, the shared fleet token)' }
      $initArgs += @('--hub', $HubUrl)
    }
    & $node @initArgs
    if ($LASTEXITCODE -ne 0) { throw "config init failed ($LASTEXITCODE)" }
  } else {
    Write-Host "[carbonsync] keeping existing $cfgPath"
  }
} finally {
  Pop-Location
}

Write-Host "[carbonsync] registering scheduled task $taskName..."
$action = New-ScheduledTaskAction -Execute $node `
  -Argument "`"$installDir\src\index.js`" run --config `"$cfgPath`"" `
  -WorkingDirectory $installDir
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet `
  -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
  -ExecutionTimeLimit ([TimeSpan]::Zero) `
  -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
  -Settings $settings -Principal $principal -Force | Out-Null

# bounce it so a re-install picks up new code (PID-file logic kills any stale instance)
Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
Start-ScheduledTask -TaskName $taskName

Start-Sleep -Seconds 2
Write-Host '[carbonsync] verifying...'
try {
  $tokenJson = Get-Content (Join-Path $installDir 'mcp-token.json') | ConvertFrom-Json
  $status = Invoke-RestMethod -Uri "http://127.0.0.1:$($tokenJson.apiPort)/v1/status" `
    -Headers @{ Authorization = "Bearer $($tokenJson.token)" } -TimeoutSec 5
  Write-Host "[carbonsync] OK -- $($status.device) ($($status.role)) v$($status.version), $($status.folders.Count) folder(s)"
} catch {
  Write-Warning "daemon not answering yet -- check $installDir\state\daemon.log ($_)"
}
