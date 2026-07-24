# Script purpose: silently uninstall the LEGACY per-user Yandex Telemost software for the
# CURRENT user, intended to be deployed as a GPO *User Configuration -> Logon* PowerShell
# script in a centrally managed domain.
#
# Why a user logon script (and not a machine startup script):
#   The software is installed PER-USER (into the user profile), and its bookkeeping lives in
#   HKCU (HKEY_CURRENT_USER). A user logon script runs in the security context of the logging-on
#   user, so HKCU already points at the right hive and no elevation / HKEY_USERS enumeration is
#   required. GPO re-applies the script for every user at every logon, which is how each user on
#   every centrally managed PC is covered.
# 
# To run with GPO, use either of:
#   1. GPO → User Configuration → Policies → Windows Settings → Scripts → Logon
#      To user it regulary
#
#   2. GPO → User Configuration → Preferences → Control Panel Settings → Scheduled Tasks
#      with trigger «At logon» and checkbox «Delete task when no longer scheduled»
#
# Logic:
#   1. Read the legacy installer path from HKCU:
#        HKCU\Software\Yandex\Yandex.Telemost.Installer :: InstallerPath
#      If it is missing, there is nothing to remove -> exit cleanly.
#   2. Build the silent uninstall command line from that path:
#        "C:\...\TelemostInstaller.exe" -uninstallcomplete -silent
#      Each required argument is appended ONLY if it is not already present (idempotent).
#   3. If Telemost is currently running, stop its process(es) first (the uninstaller cannot
#      run while the app holds its files open), then wait briefly for file locks to release.
#   4. Launch the uninstaller with the corrected command line and wait for completion.
#
# NOTE: This file is intentionally ASCII-only (no Cyrillic / smart punctuation) so it parses
#       correctly under Windows PowerShell 5.1 even if the UTF-8 BOM is stripped during transfer.

#Requires -Version 5.1

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

# --- Configuration ---
# Registry location of the legacy per-user installer path.
$InstallerSubKey  = 'Software\Yandex\Yandex.Telemost.Installer'
$InstallerName    = 'InstallerPath'

# Silent-uninstall arguments to guarantee on the command line (added individually if missing).
$RequiredArgs     = @('-uninstallcomplete', '-silent')

# Processes that hold Telemost files open and must be stopped before the uninstall.
$ProcessNames     = @('YandexTelemost', 'TelemostInstaller', 'Yandex.Telemost')
$ProcessWaitSec   = 5

# Per-user log file (logon scripts run hidden, so leave an audit trail on disk).
$LogDir           = Join-Path $env:LOCALAPPDATA 'YandexTelemostCleanup'
$LogFile          = Join-Path $LogDir 'remove_oldexe_telemost.log'

function Write-Log {
    param(
        [Parameter(Mandatory)] [string] $Message,
        [ValidateSet('INFO', 'WARN', 'ERROR')] [string] $Level = 'INFO'
    )
    $stamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
    $line  = "[$stamp] [$Level] $Message"
    Write-Host $line
    try {
        Add-Content -LiteralPath $LogFile -Value $line -Encoding UTF8 -ErrorAction Stop
    } catch {
        # Logging must never break the cleanup itself.
    }
}

# Reads a single registry value, returning $null (with a log line) when the key/value is absent.
function Get-RegistryValueOrNull {
    param(
        [Parameter(Mandatory)] [string] $Path,
        [Parameter(Mandatory)] [string] $Name
    )
    try {
        if (-not (Test-Path -LiteralPath $Path)) {
            Write-Log "Registry branch not found: $Path"
            return $null
        }
        $item  = Get-ItemProperty -LiteralPath $Path -Name $Name -ErrorAction Stop
        $value = $item.$Name
        if ($null -eq $value -or ($value -is [string] -and $value.Trim() -eq '')) {
            Write-Log "Registry value '$Name' is empty in $Path"
            return $null
        }
        return $value
    } catch [System.Management.Automation.PSArgumentException] {
        Write-Log "Registry value '$Name' not found in $Path"
        return $null
    } catch [System.Management.Automation.ItemNotFoundException] {
        Write-Log "Registry value '$Name' not found in $Path"
        return $null
    } catch {
        Write-Log "Failed to read '$Name' from $Path : $($_.Exception.Message)" 'WARN'
        return $null
    }
}

# Stops any running Telemost process so the uninstaller can proceed. Runs in user context, so
# only this user's processes are affected.
function Stop-TelemostProcesses {
    Write-Log "Checking for running Telemost processes: $($ProcessNames -join ', ')"
    $running = Get-Process -ErrorAction SilentlyContinue |
        Where-Object { $ProcessNames -contains $_.ProcessName }

    if (-not $running) {
        Write-Log "No Telemost process is running."
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

# Appends each required argument to the argument string only when it is not already present.
# Returns the corrected argument string.
function Add-MissingArguments {
    param(
        [Parameter(Mandatory)] [AllowEmptyString()] [string] $Arguments,
        [Parameter(Mandatory)] [string[]] $Required
    )
    $result = $Arguments.Trim()
    foreach ($arg in $Required) {
        # Match the flag as a whole token to avoid false positives on substrings.
        if ($result -match ('(^|\s)' + [regex]::Escape($arg) + '($|\s)')) {
            Write-Log "Argument '$arg' already present - keeping as is."
        } else {
            $result = ($result + ' ' + $arg).Trim()
            Write-Log "Appended argument '$arg'."
        }
    }
    return $result
}

try {
    if (-not (Test-Path -LiteralPath $LogDir)) {
        New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
    }
    Write-Log "=== Legacy Telemost cleanup started for user '$env:USERNAME' on '$env:COMPUTERNAME' ==="

    # --- Step 1: check the legacy installer and grab the InstallerPath ---
    $installerPath = Get-RegistryValueOrNull -Path "HKCU:\$InstallerSubKey" -Name $InstallerName
    if (-not $installerPath) {
        Write-Log "Legacy installer entry (HKCU\$InstallerSubKey :: $InstallerName) not found - nothing to remove."
        Write-Log "=== Finished (no action) ==="
        exit 0
    }
    $installerPath = $installerPath.Trim()
    Write-Log "Legacy installer path: $installerPath"

    if ($installerPath -notmatch '\.exe$') {
        Write-Log "InstallerPath does not point to a .exe: $installerPath - aborting to avoid an unexpected action." 'WARN'
        exit 1
    }
    if (-not (Test-Path -LiteralPath $installerPath)) {
        Write-Log "Installer executable does not exist on disk: $installerPath - nothing to run." 'WARN'
        exit 0
    }

    # --- Step 2: build the silent uninstall command line ---
    # Start from an empty argument set and add each required flag if it is missing.
    $exeArgs = Add-MissingArguments -Arguments '' -Required $RequiredArgs
    Write-Log "Final uninstall command: `"$installerPath`" $exeArgs"

    # --- Step 3: stop Telemost if it is currently running ---
    Stop-TelemostProcesses

    # --- Step 4: run the uninstaller and wait for completion ---
    Write-Log "Launching the legacy Telemost uninstaller..."
    $proc = Start-Process -FilePath $installerPath -ArgumentList $exeArgs -Wait -PassThru
    Write-Log "Uninstaller exited with code $($proc.ExitCode)."

    if ($proc.ExitCode -eq 0) {
        Write-Log "=== Finished: legacy Telemost removed successfully ==="
    } else {
        Write-Log "=== Finished: uninstaller returned a non-zero exit code ===" 'WARN'
    }
    exit $proc.ExitCode
} catch {
    Write-Log "Unhandled error: $($_.Exception.Message)" 'ERROR'
    if ($_.InvocationInfo) {
        Write-Log $_.InvocationInfo.PositionMessage 'ERROR'
    }
    exit 1
}
