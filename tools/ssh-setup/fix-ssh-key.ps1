# Fix Windows OpenSSH key auth that fails after Add-Content wrote a BOM.
# Re-writes both the admin and user authorized_keys files as UTF-8 NO BOM,
# drops the key in BOTH locations, ensures sshd_config has the admin directive,
# fixes ACLs, and restarts sshd.
#
# Run in admin PowerShell:
#   irm https://raw.githubusercontent.com/CarbonNode/CarbonSync/fix-sync-deletion-safety/tools/ssh-setup/fix-ssh-key.ps1 | iex

$pubkey = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIM6RPKmJOgxRewKJGBmJHMOp8NE4Th/ojA/C9bNCRIRL rober-dev-pc'
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

# 1. Admin file (the canonical location for admin users)
$adminPath = 'C:\ProgramData\ssh\administrators_authorized_keys'
if (-not (Test-Path 'C:\ProgramData\ssh')) { New-Item -Type Directory -Path 'C:\ProgramData\ssh' -Force | Out-Null }
[IO.File]::WriteAllText($adminPath, $pubkey + "`n", $utf8NoBom)
icacls $adminPath /inheritance:r /grant 'SYSTEM:(F)' 'BUILTIN\Administrators:(F)' | Out-Null

# 2. User file (fallback in case sshd_config admin directive is commented out)
$sshDir = "$env:USERPROFILE\.ssh"
if (-not (Test-Path $sshDir)) { New-Item -Type Directory -Path $sshDir -Force | Out-Null }
$userPath = "$sshDir\authorized_keys"
[IO.File]::WriteAllText($userPath, $pubkey + "`n", $utf8NoBom)
icacls $userPath /inheritance:r /grant "${env:USERNAME}:(F)" | Out-Null

# 3. Verify sshd_config has the admin directive uncommented
$sshdConfig = 'C:\ProgramData\ssh\sshd_config'
if (Test-Path $sshdConfig) {
    $config = Get-Content $sshdConfig -Raw
    if ($config -match '#\s*Match Group administrators') {
        Write-Host 'WARNING: sshd_config has admin Match block COMMENTED OUT. Uncommenting...'
        $patched = $config -replace '#\s*Match Group administrators', 'Match Group administrators'
        $patched = $patched -replace '#\s*AuthorizedKeysFile __PROGRAMDATA__/ssh/administrators_authorized_keys', '       AuthorizedKeysFile __PROGRAMDATA__/ssh/administrators_authorized_keys'
        [IO.File]::WriteAllText($sshdConfig, $patched, $utf8NoBom)
    }
}

# 4. Restart sshd
Restart-Service sshd

# 5. Report
Write-Host "---REPORT---"
Write-Host "Admin keys file: $adminPath"
Write-Host "  bytes: $((Get-Item $adminPath).Length)  contents:"
Get-Content $adminPath
Write-Host "User keys file: $userPath"
Write-Host "  bytes: $((Get-Item $userPath).Length)  contents:"
Get-Content $userPath
Write-Host "sshd service:"
Get-Service sshd | Format-Table -AutoSize
