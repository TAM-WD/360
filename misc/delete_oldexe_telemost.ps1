# Script purpose: remove the legacy Telemost software and update the telemost:// protocol
# handler so that Telemost links open in the new .msi client.
# Logic:
#   1. Check for the legacy installer (HKCU\...\Yandex.Telemost.Installer\InstallerPath).
#   2. Get the new Telemost version directory (HKLM has priority, otherwise HKCU from
#      Yandex.Telemost.2.Installer\InstallDir).
#   3. If the legacy installer AND the new version dir are both found, run UninstallString
#      as admin and wait for completion.
#   4. If a new install path is found, rewrite the telemost:// protocol handler.
#
# NOTE: This file is intentionally ASCII-only (no Cyrillic / smart punctuation) so it parses
#       correctly under Windows PowerShell 5.1 even if the UTF-8 BOM is stripped during transfer.

#Requires -Version 5.1

[CmdletBinding()]
param(
    # SID of the original (non-elevated) user - passed to the child process after UAC
    # so we can read that user's HKCU via HKEY_USERS\<SID>.
    [string]$OriginalUserSid
)

$ErrorActionPreference = 'Stop'

# --- Run settings ---
# $RunMode = 'Admin' -> full mode for a user WITH administrator rights:
#                       find/remove the legacy installer and update the telemost://
#                       handler in all hives (HKCR, HKLM, HKCU).
# $RunMode = 'User'  -> mode for a user WITHOUT administrator rights:
#                       UAC elevation is NOT requested; legacy lookup/removal still run with the
#                       current privileges, but the telemost:// handler is updated only in the
#                       HKCU hive (HKEY_CURRENT_USER\Software\Classes\telemost\shell\open\command).
# $AskForAdmin = $true  -> ('Admin' mode only) if not elevated, the script relaunches via UAC.
# $AskForAdmin = $false -> elevation is not requested (some actions may not be performed).
# $ForceUninstall = $true  -> remove the legacy version even if the new version InstallDir
#                             was NOT found (normally removal also requires the new version).
# $ForceUninstall = $false -> remove the legacy version only when the new version is found.
# $DoNotExit   = $true  -> on finish and on error the window waits for Enter.
# $DoNotExit   = $false -> the script exits immediately, without a pause.

$RunMode        = 'Admin'
$AskForAdmin    = $false
$ForceUninstall = $false
$DoNotExit      = $false

function Pause-Exit {
    param([int]$Code = 0)
    if ($DoNotExit) {
        Write-Host ""
        Read-Host "Press Enter to exit"
    }
    exit $Code
}

function Test-IsAdministrator {
    $current = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($current)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

# --- Self-elevation to administrator ('Admin' mode only) ---
if ($RunMode -eq 'User') {
    Write-Host "Mode 'User': running without administrator rights, elevation is not requested."
} elseif ($AskForAdmin -and -not (Test-IsAdministrator)) {
    Write-Host "Administrator rights are required. Relaunching the script with elevation..."
    $scriptPath = $MyInvocation.MyCommand.Path
    if (-not $scriptPath) {
        Write-Warning "Could not determine the script path for self-relaunch."
        Pause-Exit 1
    }
    try {
        $currentSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
        $childArgs  = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', "`"$scriptPath`"",
                        '-OriginalUserSid', $currentSid)
        if ($DoNotExit) { $childArgs = @('-NoExit') + $childArgs }
        Start-Process -FilePath 'powershell.exe' -ArgumentList $childArgs -Verb RunAs
        exit 0
    } catch {
        Write-Warning "Could not relaunch with administrator rights: $($_.Exception.Message)"
        Pause-Exit 1
    }
} elseif (-not (Test-IsAdministrator)) {
    Write-Warning "Script started without administrator rights (AskForAdmin=`$false). Some actions may not be performed."
}

function Get-RegistryValueOrNull {
    param(
        [Parameter(Mandatory)] [string] $Path,
        [Parameter(Mandatory)] [string] $Name
    )
    try {
        $item = Get-ItemProperty -Path $Path -Name $Name -ErrorAction Stop
        return $item.$Name
    } catch {
        return $null
    }
}

# Returns the array of registry paths to search for a value for the given HKCU subkey.
# If the script was elevated (-OriginalUserSid passed), HKCU now belongs to the admin,
# so we additionally check the original user's hive via HKEY_USERS\<SID>.
function Get-HkcuSearchPaths {
    param([Parameter(Mandatory)] [string] $SubPath)  # e.g. 'Software\Yandex\Yandex.Telemost.Installer'
    $paths = @("HKCU:\$SubPath")
    if ($OriginalUserSid) {
        $paths += "Registry::HKEY_USERS\$OriginalUserSid\$SubPath"
    }
    return $paths
}

# Verbose registry value read with logging:
#   - whether the branch (key) exists,
#   - whether it is readable (no AccessDenied),
#   - whether the target value exists.
# Returns the value or $null. Prints lines like [BRANCH OK]/[BRANCH MISS]/[DENIED]/[VALUE OK]/[VALUE MISS]/[ERROR].
function Read-RegistryValueVerbose {
    param(
        [Parameter(Mandatory)] [string] $Path,
        [Parameter(Mandatory)] [string] $Name
    )
    # 1. Check that the branch exists.
    $branchExists = $false
    try {
        $branchExists = Test-Path -LiteralPath $Path -ErrorAction Stop
    } catch [System.Security.SecurityException] {
        Write-Host "  [DENIED     ] No access to the branch: $Path"
        return $null
    } catch {
        Write-Host "  [ERROR      ] Branch check '$Path' failed: $($_.Exception.GetType().Name): $($_.Exception.Message)"
        return $null
    }
    if (-not $branchExists) {
        Write-Host "  [BRANCH MISS] Branch does not exist: $Path"
        return $null
    }
    Write-Host "  [BRANCH OK  ] Branch found: $Path"

    # 2. Extra check for read access to the branch properties (in case of an ACL deny on values).
    try {
        Get-ItemProperty -LiteralPath $Path -ErrorAction Stop | Out-Null
    } catch [System.Security.SecurityException] {
        Write-Host "  [DENIED     ] No rights to read branch values: $Path"
        return $null
    } catch [System.UnauthorizedAccessException] {
        Write-Host "  [DENIED     ] UnauthorizedAccess while reading branch: $Path"
        return $null
    } catch {
        # Normal situations may land here too (empty branch) - not treated as critical.
    }

    # 3. Read the specific value.
    try {
        $item  = Get-ItemProperty -LiteralPath $Path -Name $Name -ErrorAction Stop
        $value = $item.$Name
        if ($null -eq $value -or ($value -is [string] -and $value -eq '')) {
            Write-Host "  [VALUE MISS ] Entry '$Name' is empty in $Path"
            return $null
        }
        Write-Host "  [VALUE OK   ] $Path :: $Name = $value"
        return $value
    } catch [System.Security.SecurityException] {
        Write-Host "  [DENIED     ] No rights to read '$Name' in $Path"
        return $null
    } catch [System.UnauthorizedAccessException] {
        Write-Host "  [DENIED     ] UnauthorizedAccess on '$Name' in $Path"
        return $null
    } catch [System.Management.Automation.PSArgumentException] {
        Write-Host "  [VALUE MISS ] Entry '$Name' not found in $Path"
        return $null
    } catch [System.Management.Automation.ItemNotFoundException] {
        Write-Host "  [VALUE MISS ] Entry '$Name' not found in $Path"
        return $null
    } catch {
        Write-Host "  [ERROR      ] $($_.Exception.GetType().Name): $($_.Exception.Message)"
        return $null
    }
}

function Get-HkcuValue {
    param(
        [Parameter(Mandatory)] [string] $SubPath,
        [Parameter(Mandatory)] [string] $Name
    )
    foreach ($p in Get-HkcuSearchPaths -SubPath $SubPath) {
        $value = Get-RegistryValueOrNull -Path $p -Name $Name
        if ($null -ne $value) {
            Write-Host "  [HIT ] $p :: $Name = $value"
            return $value
        } else {
            Write-Host "  [MISS] $p :: $Name"
        }
    }
    return $null
}

try {
    if ($OriginalUserSid) {
        Write-Host "Started elevated; original user's HKCU: HKEY_USERS\$OriginalUserSid"
    }

    # --- Step 1: check the legacy installer and grab the UninstallString ---
    # Runs in both modes (in 'User' mode too, just without elevation).
    $oldInstallerPath = $null
    $uninstallString  = $null
    Write-Host "[Step 1] Looking for the legacy installer Yandex.Telemost.Installer..."
    $oldInstallerPath = Get-HkcuValue -SubPath 'Software\Yandex\Yandex.Telemost.Installer' -Name 'InstallerPath'

    if ($oldInstallerPath) {
        Write-Host "Legacy installer found: $oldInstallerPath"
        Write-Host "[Step 1] Looking for UninstallString..."
        $uninstallString = Get-HkcuValue `
            -SubPath 'Software\Microsoft\Windows\CurrentVersion\Uninstall\YandexTelemost' `
            -Name 'UninstallString'
        if ($uninstallString) {
            $uninstallString = $uninstallString.TrimEnd() + ' -silent'
            Write-Host "UninstallString: $uninstallString"
        } else {
            # Fallback: the InstallerPath value itself points to the legacy installer exe,
            # so build the uninstall command from it: "<InstallerPath>" -uninstallcomplete -silent.
            Write-Host "UninstallString not found in HKCU\...\Uninstall\YandexTelemost - building from InstallerPath."
            $uninstallString = '"{0}" -uninstallcomplete -silent' -f $oldInstallerPath.Trim()
            Write-Host "[Step 1] Fallback UninstallString from InstallerPath: $uninstallString"
        }
    } else {
        Write-Host "Legacy installer (Yandex.Telemost.Installer) not found - skipping removal."
    }

    # --- Step 2: find the new version InstallDir (HKLM priority, then HKCU) ---
    Write-Host "[Step 2] Looking for the new Telemost version InstallDir..."
    Write-Host "[Step 2] PowerShell process: PID=$PID, 64-bit=$([Environment]::Is64BitProcess), OS 64-bit=$([Environment]::Is64BitOperatingSystem)"

    # Check both HKLM branches: native and WOW6432Node - in case of bitness redirection.
    $hklmCandidates = @(
        'HKLM:\SOFTWARE\Yandex\Yandex.Telemost.2.Installer',
        'HKLM:\SOFTWARE\WOW6432Node\Yandex\Yandex.Telemost.2.Installer'
    )
    $installDirHKLM = $null
    foreach ($p in $hklmCandidates) {
        Write-Host "[Step 2] Checking HKLM: $p :: InstallDir"
        $v = Read-RegistryValueVerbose -Path $p -Name 'InstallDir'
        if ($v) { $installDirHKLM = $v; break }
    }

    Write-Host "[Step 2] Checking HKCU (including the original user's hive when elevated)..."
    $installDirHKCU = $null
    foreach ($p in (Get-HkcuSearchPaths -SubPath 'Software\Yandex\Yandex.Telemost.2.Installer')) {
        Write-Host "[Step 2] Checking HKCU/HKU: $p :: InstallDir"
        $v = Read-RegistryValueVerbose -Path $p -Name 'InstallDir'
        if ($v) { $installDirHKCU = $v; break }
    }

    if ($installDirHKLM) {
        $installDir = $installDirHKLM
        Write-Host "[Step 2] Source chosen: HKLM"
    } elseif ($installDirHKCU) {
        $installDir = $installDirHKCU
        Write-Host "[Step 2] Source chosen: HKCU"
    } else {
        $installDir = $null
    }

    if ($installDir) {
        Write-Host "New version InstallDir: $installDir"
    } else {
        Write-Host "New Telemost version InstallDir not found in HKLM or HKCU."
    }

    # --- Step 3: remove the legacy version ---
    # Normally requires: legacy installer found, uninstall command known, and the new version present.
    # With $ForceUninstall = $true the new-version requirement ($installDir) is dropped.
    if ($oldInstallerPath -and $uninstallString -and ($installDir -or $ForceUninstall)) {
        if (-not $installDir -and $ForceUninstall) {
            Write-Host "Removing the legacy Telemost version (forced: new version not found, ForceUninstall=`$true)..."
        } else {
            Write-Host "Removing the legacy Telemost version..."
        }

        # Parse an UninstallString like: "C:\...\TelemostInstaller.exe" -uninstallcomplete -silent
        $exePath = $null
        $exeArgs = ''
        if ($uninstallString -match '^\s*"([^"]+)"\s*(.*)$') {
            $exePath = $Matches[1]
            $exeArgs = $Matches[2]
        } elseif ($uninstallString -match '^\s*(\S+)\s*(.*)$') {
            $exePath = $Matches[1]
            $exeArgs = $Matches[2]
        }

        if (-not $exePath -or -not (Test-Path -LiteralPath $exePath)) {
            Write-Warning "Uninstaller executable not found: $exePath"
        } else {
            $startArgs = @{
                FilePath = $exePath
                Wait     = $true
            }
            if ($exeArgs.Trim()) { $startArgs.ArgumentList = $exeArgs.Trim() }

            try {
                Start-Process @startArgs
                Write-Host "Uninstall completed."
            } catch {
                Write-Warning "Error while launching the uninstaller: $($_.Exception.Message)"
            }
        }
    } else {
        Write-Host "Legacy version removal skipped. Reason(s):"
        if (-not $oldInstallerPath) {
            Write-Host "  - legacy installer not found (HKCU\...\Yandex.Telemost.Installer :: InstallerPath missing)"
        }
        if (-not $uninstallString) {
            Write-Host "  - uninstall command not determined (no UninstallString key and no InstallerPath fallback)"
        }
        if (-not $installDir -and -not $ForceUninstall) {
            Write-Host "  - new version InstallDir not found (set `$ForceUninstall = `$true to remove anyway)"
        }
    }

    # --- Step 4: update the telemost:// protocol handler ---
    if ($installDir) {
        $exeFullPath = Join-Path -Path $installDir -ChildPath 'YandexTelemost.exe'
        $newCommand  = '"{0}" --conf-url="%1"' -f $exeFullPath

        # In 'User' mode (no admin rights) update only the HKCU protocol handler hive;
        # in 'Admin' mode update all hives (HKCR, HKLM, HKCU). Writes are caught per key so
        # one failure does not abort the rest (esp. HKCU).
        if ($RunMode -eq 'User') {
            $protocolKeys = @(
                'Registry::HKEY_CURRENT_USER\Software\Classes\telemost\shell\open\command'
            )
        } else {
            $protocolKeys = @(
                'Registry::HKEY_CLASSES_ROOT\telemost\shell\open\command',
                'Registry::HKEY_LOCAL_MACHINE\SOFTWARE\Classes\telemost\shell\open\command',
                'Registry::HKEY_CURRENT_USER\Software\Classes\telemost\shell\open\command'
            )
        }
        foreach ($key in $protocolKeys) {
            if (Test-Path -LiteralPath $key) {
                try {
                    Set-ItemProperty -LiteralPath $key -Name '(Default)' -Value $newCommand -ErrorAction Stop
                    Write-Host "Updated $key -> $newCommand"
                } catch {
                    Write-Warning "Could not update $key : $($_.Exception.Message)"
                }
            } else {
                Write-Host "$key does not exist - skipping."
            }
        }
    } else {
        Write-Host "Skipping the telemost:// handler update - no InstallDir."
    }

    Write-Host "Done."
} catch {
    Write-Host ""
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.InvocationInfo) {
        Write-Host $_.InvocationInfo.PositionMessage -ForegroundColor Red
    }
} finally {
    Pause-Exit
}
