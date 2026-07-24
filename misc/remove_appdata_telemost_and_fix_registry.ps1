<#
.SYNOPSIS
    Removes per-user installations of Yandex Telemost for the CURRENT user,
    keeps the per-machine installation in Program Files, and fixes the
    telemost:// protocol association so it points at the machine client and
    is served from HKLM.

.DESCRIPTION
    Intended to run IN THE USER CONTEXT (e.g. a GPO User Configuration ->
    Logon Script), WITHOUT elevation to administrator. Elevation is not
    required because:
      - a per-user MSI is registered in the current user's HKCU, and its
        removal (msiexec /x) runs in that same user's context;
      - the legacy exe Telemost also lives in the user profile and is removed
        with the user's own rights;
      - the per-user telemost:// association lives in HKCU\Software\Classes
        and is edited without administrator rights.

    LOGIC:
      1. Verify the per-machine Telemost is installed in HKLM
         (Yandex.Telemost.2.Installer :: InstallDir points to Program Files).
         If there is no machine installation - remove NOTHING: otherwise the
         user would be left with no client at all. Just report and exit.
      2. Look for OTHER (per-user) installations for the current user:
           - per-user MSI:  HKCU\Software\Yandex\Yandex.Telemost.2.Installer :: ProductCode
           - legacy exe:    HKCU\Software\Yandex\Yandex.Telemost.Installer   :: InstallerPath
      3. If found - remove them (stopping Telemost processes first), leaving the
         machine client in Program Files untouched.
      3b. Regardless of whether clients were found, remove the per-user
          telemost:// association (HKCU\Software\Classes\telemost): it may be an
          orphan left by a long-gone per-user client and still shadow the HKLM
          entry.
      4. Verify the registry: telemost:// now resolves from HKLM and points at
         an existing exe in Program Files (no shadowing entry remains in HKCU).

.NOTES
    PowerShell 5.1+. Administrator rights are NOT required.
    This file is intentionally ASCII-only (no Cyrillic / smart punctuation) so
    it parses correctly under Windows PowerShell 5.1 even if the UTF-8 BOM is
    stripped during transfer.
#>

#Requires -Version 5.1

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

# --- Configuration ----------------------------------------------------------

# Per-machine installation of the new MSI client. Check both branches in case
# of bitness redirection (WOW6432Node).
$MachineInstallerKeys = @(
    'HKLM:\SOFTWARE\Yandex\Yandex.Telemost.2.Installer',
    'HKLM:\SOFTWARE\WOW6432Node\Yandex\Yandex.Telemost.2.Installer'
)
$InstallDirValueName  = 'InstallDir'
$ProductCodeValueName = 'ProductCode'
$ClientExeName        = 'YandexTelemost.exe'

# Per-user MSI client (HKCU).
$PeruserMsiKey        = 'HKCU:\Software\Yandex\Yandex.Telemost.2.Installer'

# Legacy exe client (HKCU).
$LegacyExeKey         = 'HKCU:\Software\Yandex\Yandex.Telemost.Installer'
$LegacyExeValueName   = 'InstallerPath'
$LegacyUninstallArgs  = @('-uninstallcomplete', '-silent')

# Protocol association. The relative path is identical in HKLM and HKCU.
$ProtocolRelKey       = 'Software\Classes\telemost'
$ProtocolCommandRel   = "$ProtocolRelKey\shell\open\command"
$HklmProtocolCommand  = "HKLM:\$ProtocolCommandRel"
$HkcuProtocolRoot     = "HKCU:\$ProtocolRelKey"
$HkcuProtocolCommand  = "HKCU:\$ProtocolCommandRel"

# Processes that hold Telemost files open and block removal.
$ProcessNames         = @('Telemost', 'Yandex.Telemost', 'YandexTelemost', 'TelemostInstaller')
$ProcessWaitSec       = 5

# --- Logging ----------------------------------------------------------------
# The script runs in the user context, so log into the user's LOCALAPPDATA.
$LogDir  = Join-Path $env:LOCALAPPDATA 'YandexTelemostCleanup'
$LogFile = Join-Path $LogDir 'remove_appdata_telemost.log'

function Write-Log {
    param(
        [Parameter(Mandatory)] [string] $Message,
        [ValidateSet('INFO', 'OK', 'WARN', 'ERROR')] [string] $Level = 'INFO'
    )
    $line = "{0} [{1}] {2}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Level, $Message
    Write-Host $line
    try { Add-Content -LiteralPath $LogFile -Value $line -Encoding UTF8 -ErrorAction Stop } catch { }
}

# Reads a single registry value, returning $null when the key/value is absent.
function Get-RegistryValueOrNull {
    param(
        [Parameter(Mandatory)] [string] $Path,
        [Parameter(Mandatory)] [string] $Name
    )
    try {
        if (-not (Test-Path -LiteralPath $Path)) { return $null }
        $value = (Get-ItemProperty -LiteralPath $Path -Name $Name -ErrorAction Stop).$Name
        if ($null -eq $value -or ($value -is [string] -and $value.Trim() -eq '')) { return $null }
        if ($value -is [string]) { return $value.Trim() }
        return $value
    } catch {
        return $null
    }
}

# Checks that a string is a valid GUID {XXXXXXXX-...}. An MSI ProductCode is
# always in braces; this also guards against argument injection into msiexec.
function Test-ProductCode {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) { return $false }
    return $Value -match '^\{[0-9A-Fa-f]{8}-([0-9A-Fa-f]{4}-){3}[0-9A-Fa-f]{12}\}$'
}

# Extracts the .exe path from a shell\open\command command string.
function Get-ExeFromCommand {
    param([string]$Command)
    if ([string]::IsNullOrWhiteSpace($Command)) { return $null }
    if ($Command -match '^\s*"([^"]+)"') { return [Environment]::ExpandEnvironmentVariables($Matches[1]) }
    if ($Command -match '^\s*(\S+)')     { return [Environment]::ExpandEnvironmentVariables($Matches[1]) }
    return $null
}

# Stops the current user's Telemost processes to release files before removal.
# Runs in the user context, so only this user's processes are affected.
function Stop-TelemostProcesses {
    $running = Get-Process -ErrorAction SilentlyContinue |
        Where-Object { $ProcessNames -contains $_.ProcessName }
    if (-not $running) {
        Write-Log "No running Telemost process found."
        return
    }
    foreach ($proc in $running) {
        try {
            Stop-Process -Id $proc.Id -Force -ErrorAction Stop
            Write-Log "Stopped process $($proc.ProcessName) (PID $($proc.Id))."
        } catch {
            Write-Log "Could not stop $($proc.ProcessName) (PID $($proc.Id)): $($_.Exception.Message)" 'WARN'
        }
    }
    Write-Log "Waiting $ProcessWaitSec s for file locks to release..."
    Start-Sleep -Seconds $ProcessWaitSec
}

# ============================================================================
#  Main flow
# ============================================================================
try {
    if (-not (Test-Path -LiteralPath $LogDir)) {
        New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
    }
    Write-Log "=== Per-user Telemost cleanup for '$env:USERNAME' on '$env:COMPUTERNAME' ==="

    # --- Step 1: machine installation in HKLM (kept) -----------------------
    Write-Log "[Step 1] Checking the per-machine installation in HKLM."
    $machineInstallDir  = $null
    $machineProductCode = $null
    foreach ($key in $MachineInstallerKeys) {
        $dir = Get-RegistryValueOrNull -Path $key -Name $InstallDirValueName
        if ($dir) {
            $machineInstallDir  = $dir
            $machineProductCode = Get-RegistryValueOrNull -Path $key -Name $ProductCodeValueName
            Write-Log "  Machine InstallDir found in '$key': $machineInstallDir"
            break
        }
        Write-Log "  No InstallDir in '$key'."
    }

    if (-not $machineInstallDir) {
        Write-Log "No per-machine Telemost installation found in HKLM. To avoid leaving" 'WARN'
        Write-Log "the user with no client, per-user installations are NOT removed. Exit." 'WARN'
        Write-Log "=== Finished (no changes) ==="
        exit 0
    }

    $machineExe = Join-Path $machineInstallDir $ClientExeName
    if (Test-Path -LiteralPath $machineExe) {
        Write-Log "  Machine client present on disk: $machineExe" 'OK'
    } else {
        Write-Log "  WARNING: machine exe not found at '$machineExe'. Registry exists," 'WARN'
        Write-Log "  but the file does not - removing per-user clients may leave the" 'WARN'
        Write-Log "  system without a working client. Continuing, but note it in Step 4." 'WARN'
    }

    # --- Step 2: find per-user installations for the current user ----------
    Write-Log "[Step 2] Looking for other (per-user) Telemost installations for the user."

    # 2a. per-user MSI (HKCU).
    $peruserProductCode = Get-RegistryValueOrNull -Path $PeruserMsiKey -Name $ProductCodeValueName
    $peruserInstallDir  = Get-RegistryValueOrNull -Path $PeruserMsiKey -Name $InstallDirValueName
    $hasPeruserMsi      = $false
    if ($peruserProductCode) {
        if ($machineProductCode -and ($peruserProductCode -ieq $machineProductCode)) {
            # The HKCU entry points at the same product as HKLM - this is the
            # machine registration, not a separate per-user client. Leave it.
            Write-Log "  HKCU ProductCode matches the machine one ($peruserProductCode) - this"
            Write-Log "  is the machine registration; no standalone per-user MSI is present."
        } elseif (-not (Test-ProductCode -Value $peruserProductCode)) {
            Write-Log "  HKCU ProductCode '$peruserProductCode' does not look like a GUID - skip." 'WARN'
        } else {
            $hasPeruserMsi = $true
            Write-Log "  Found per-user MSI: ProductCode=$peruserProductCode InstallDir='$peruserInstallDir'."
        }
    } else {
        Write-Log "  No per-user MSI (HKCU\...\Yandex.Telemost.2.Installer :: ProductCode)."
    }

    # 2b. legacy exe (HKCU).
    $legacyExePath = Get-RegistryValueOrNull -Path $LegacyExeKey -Name $LegacyExeValueName
    $hasLegacyExe  = $false
    if ($legacyExePath) {
        if ($legacyExePath -notmatch '\.exe$') {
            Write-Log "  Legacy InstallerPath does not point to a .exe ('$legacyExePath') - skip." 'WARN'
        } elseif (-not (Test-Path -LiteralPath $legacyExePath)) {
            Write-Log "  Legacy exe is in the registry but the file is missing ('$legacyExePath')." 'WARN'
            Write-Log "  Nothing to run for removal; only the association will be cleaned in Step 3b."
        } else {
            $hasLegacyExe = $true
            Write-Log "  Found legacy exe client: $legacyExePath"
        }
    } else {
        Write-Log "  No legacy exe client (HKCU\...\Yandex.Telemost.Installer :: InstallerPath)."
    }

    # --- Step 3: remove the found per-user clients -------------------------
    if (-not $hasPeruserMsi -and -not $hasLegacyExe) {
        Write-Log "[Step 3] No per-user clients to remove."
    } else {
        Write-Log "[Step 3] Removing per-user clients (the machine one in Program Files is kept)."
        Stop-TelemostProcesses

        if ($hasPeruserMsi) {
            Write-Log "  Removing per-user MSI: msiexec /x `"$peruserProductCode`" /qn /norestart"
            try {
                $p = Start-Process msiexec.exe `
                        -ArgumentList '/x', $peruserProductCode, '/qn', '/norestart' `
                        -Wait -PassThru
                switch ($p.ExitCode) {
                    0       { Write-Log "  per-user MSI removed successfully (0)." 'OK' }
                    1605    { Write-Log "  product is already not installed (1605) - skip." }
                    3010    { Write-Log "  removed, reboot required (3010)." 'OK' }
                    default { Write-Log "  msiexec returned code $($p.ExitCode)." 'WARN' }
                }
            } catch {
                Write-Log "  Failed to launch msiexec: $($_.Exception.Message)" 'ERROR'
            }
        }

        if ($hasLegacyExe) {
            Write-Log "  Removing legacy exe: `"$legacyExePath`" $($LegacyUninstallArgs -join ' ')"
            try {
                $p = Start-Process -FilePath $legacyExePath -ArgumentList $LegacyUninstallArgs -Wait -PassThru
                if ($p.ExitCode -eq 0) {
                    Write-Log "  legacy exe removed successfully (0)." 'OK'
                } else {
                    Write-Log "  uninstaller returned code $($p.ExitCode)." 'WARN'
                }
            } catch {
                Write-Log "  Failed to launch the uninstaller: $($_.Exception.Message)" 'ERROR'
            }
        }
    }

    # Remove the per-user telemost:// association REGARDLESS of whether clients
    # were found: the HKCU\Software\Classes\telemost entry may be an orphan left
    # by a long-gone per-user client and still shadow the HKLM entry. Since the
    # machine installation (Step 1) is definitely present, after cleanup the
    # user has a working fallback via HKLM.
    Write-Log "[Step 3b] Cleaning the per-user telemost:// association (HKCU), if any."
    if (Test-Path -LiteralPath $HkcuProtocolRoot) {
        try {
            Remove-Item -LiteralPath $HkcuProtocolRoot -Recurse -Force -ErrorAction Stop
            Write-Log "  Removed the per-user association '$HkcuProtocolRoot'." 'OK'
        } catch {
            Write-Log "  Could not remove '$HkcuProtocolRoot': $($_.Exception.Message)" 'ERROR'
        }
    } else {
        Write-Log "  No per-user association '$HkcuProtocolRoot' - nothing to clean."
    }

    # --- Step 4: verify the telemost:// association ------------------------
    Write-Log "[Step 4] Verifying telemost:// is served from HKLM and points at an"
    Write-Log "         existing client in Program Files."
    $ok = $true

    # 4a. No shadowing entry must remain in HKCU.
    if (Test-Path -LiteralPath $HkcuProtocolCommand) {
        $hkcuCmd = Get-RegistryValueOrNull -Path $HkcuProtocolCommand -Name '(default)'
        Write-Log "  HKCU still holds a telemost:// association -> $hkcuCmd" 'WARN'
        Write-Log "  While it exists, it takes priority over HKLM for this user." 'WARN'
        $ok = $false
    } else {
        Write-Log "  No shadowing telemost:// entry in HKCU - HKLM takes priority." 'OK'
    }

    # 4b. The HKLM entry must exist.
    if (-not (Test-Path -LiteralPath $HklmProtocolCommand)) {
        Write-Log "  HKLM has no '$ProtocolCommandRel' - the machine association is missing." 'ERROR'
        $ok = $false
    } else {
        $hklmCmd = Get-RegistryValueOrNull -Path $HklmProtocolCommand -Name '(default)'
        Write-Log "  HKLM telemost:// association -> $hklmCmd"
        $hklmExe = Get-ExeFromCommand -Command $hklmCmd

        if (-not $hklmExe) {
            Write-Log "  Could not extract the .exe path from the HKLM command." 'ERROR'
            $ok = $false
        } elseif (-not (Test-Path -LiteralPath $hklmExe)) {
            Write-Log "  The HKLM client is not present on disk: $hklmExe" 'ERROR'
            $ok = $false
        } else {
            Write-Log "  The HKLM client exists: $hklmExe" 'OK'
            # 4c. And it must be the machine client from Program Files (matching HKLM InstallDir).
            $expectedExe = Join-Path $machineInstallDir $ClientExeName
            if ($hklmExe -ieq $expectedExe) {
                Write-Log "  Path matches the machine InstallDir from HKLM - association is correct." 'OK'
            } else {
                Write-Log "  WARNING: the HKLM command ('$hklmExe') does not match the expected" 'WARN'
                Write-Log "  machine path ('$expectedExe'). Check manually." 'WARN'
            }
        }
    }

    if ($ok) {
        Write-Log "=== Done: per-user clients cleared, telemost:// resolves from HKLM to Program Files ==="
        exit 0
    } else {
        Write-Log "=== Finished with association warnings (see WARN/ERROR above) ===" 'WARN'
        exit 1
    }
} catch {
    Write-Log "Unhandled error: $($_.Exception.Message)" 'ERROR'
    if ($_.InvocationInfo) { Write-Log $_.InvocationInfo.PositionMessage 'ERROR' }
    exit 1
}
