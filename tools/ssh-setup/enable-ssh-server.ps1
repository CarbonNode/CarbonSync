# CarbonSync SSH server enabler.
# Run in an Administrator PowerShell on the target machine.
# Installs OpenSSH, starts it, opens firewall, drops MainGamingRig's pubkey
# into the right authorized_keys file (admin vs non-admin), fixes ACLs.
#
# One-line invocation (admin PowerShell):
#   irm https://raw.githubusercontent.com/CarbonNode/CarbonSync/fix-sync-deletion-safety/tools/ssh-setup/enable-ssh-server.ps1 | iex

$pubkey = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIM6RPKmJOgxRewKJGBmJHMOp8NE4Th/ojA/C9bNCRIRL rober-dev-pc'

Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0 | Out-Null
Start-Service sshd
Set-Service -Name sshd -StartupType 'Automatic'
if (-not (Get-NetFirewallRule -Name "OpenSSH-Server-In-TCP" -ErrorAction SilentlyContinue)) {
    New-NetFirewallRule -Name 'OpenSSH-Server-In-TCP' -DisplayName 'OpenSSH Server (sshd)' -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22 | Out-Null
}

$user = $env:USERNAME
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if ($isAdmin) {
    $authPath = 'C:\ProgramData\ssh\administrators_authorized_keys'
    if (-not (Test-Path 'C:\ProgramData\ssh')) { New-Item -Type Directory -Path 'C:\ProgramData\ssh' -Force | Out-Null }
    if (-not (Test-Path $authPath)) { New-Item -Type File -Path $authPath -Force | Out-Null }
    $existing = Get-Content $authPath -Raw -ErrorAction SilentlyContinue
    if ($existing -notmatch [regex]::Escape($pubkey)) { Add-Content -Path $authPath -Value $pubkey }
    icacls $authPath /inheritance:r /grant 'SYSTEM:(F)' 'BUILTIN\Administrators:(F)' | Out-Null
} else {
    $sshDir = "$env:USERPROFILE\.ssh"
    if (-not (Test-Path $sshDir)) { New-Item -Type Directory -Path $sshDir -Force | Out-Null }
    $authPath = "$sshDir\authorized_keys"
    $existing = Get-Content $authPath -Raw -ErrorAction SilentlyContinue
    if ($existing -notmatch [regex]::Escape($pubkey)) { Add-Content -Path $authPath -Value $pubkey }
    icacls $authPath /inheritance:r /grant "${user}:(F)" | Out-Null
}

Restart-Service sshd

Write-Host "---REPORT---"
Write-Host "User: $user (admin: $isAdmin)"
Write-Host "Hostname: $env:COMPUTERNAME"
Write-Host "Authorized keys file: $authPath"
Get-Service sshd | Format-Table -AutoSize
Get-NetTCPConnection -LocalPort 22 -State Listen -ErrorAction SilentlyContinue | Select-Object LocalAddress,LocalPort | Format-Table -AutoSize
