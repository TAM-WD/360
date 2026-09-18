<#
.SYNOPSIS
    Silently removes selected Yandex Messenger installations through Group Policy.

.DESCRIPTION
    This script has two deliberately separated execution modes:

    * User mode removes the per-user EXE and/or per-user MSI installation. Deploy
      it through User Configuration so it runs under every signing-in user's own
      identity.
    * Machine mode removes the per-machine MSI installation. Deploy it through
      Computer Configuration as a startup script running under LocalSystem.

    Run the same script from both GPO sections. Missing clients and uninstall
    errors are logged but never produce a failing script exit code. The script is
    compatible with Windows PowerShell 2.0 and does not use ConvertFrom-Json.

.PARAMETER Context
    Use User for a user logon script and Machine for a computer startup script.
    Auto maps LocalSystem to Machine and any other identity to User. Explicit
    values are recommended for deterministic GPO behavior.

.PARAMETER Clients
    Comma-separated client identifiers to inspect. Supported identifiers are:
    UserExe, CurrentUserMsi, LocalMachineMsi, and All.

    If omitted, User mode selects UserExe and CurrentUserMsi, while Machine mode
    selects LocalMachineMsi. A client that is incompatible with the selected
    context is skipped safely.

.PARAMETER LogDirectory
    Optional log directory. Machine mode defaults to ProgramData and User mode
    defaults to LocalAppData. If that directory cannot be used, logging falls
    back to the process temporary directory.

.PARAMETER UserMsiInstallPath
    Optional full path to the per-user MSI installation directory containing
    .installInfo.json, for example D:\Apps\Yandex Messenger. An empty or
    whitespace-only value uses LocalAppData\Programs\Yandex Messenger.
    Used only for CurrentUserMsi in User context.

.PARAMETER MachineMsiInstallPath
    Optional full path to the per-machine MSI installation directory containing
    .installInfo.json, for example D:\Apps\Yandex Messenger. An empty or
    whitespace-only value uses native Program Files\Yandex Messenger.
    Used only for LocalMachineMsi in Machine context.

.PARAMETER ProcessTimeoutSeconds
    Maximum time to wait for one uninstaller. A bounded wait prevents a damaged
    uninstaller from indefinitely blocking computer startup or user sign-in.

.PARAMETER MsiRetryCount
    Number of retries when Windows Installer returns 1618 because another MSI
    transaction is in progress.

.PARAMETER MsiRetryDelaySeconds
    Delay between Windows Installer busy retries.

.EXAMPLE
    powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File \\domain.example\SYSVOL\domain.example\scripts\remove_yandex_messenger.ps1 -Context User -Clients "UserExe,CurrentUserMsi"

.EXAMPLE
    powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File \\domain.example\SYSVOL\domain.example\scripts\remove_yandex_messenger.ps1 -Context Machine -Clients "LocalMachineMsi"

.EXAMPLE
    powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File \\domain.example\SYSVOL\domain.example\scripts\remove_yandex_messenger.ps1 -Context User -Clients "CurrentUserMsi" -UserMsiInstallPath "D:\User Apps\Yandex Messenger"

.EXAMPLE
    powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File \\domain.example\SYSVOL\domain.example\scripts\remove_yandex_messenger.ps1 -Context Machine -Clients "LocalMachineMsi" -MachineMsiInstallPath "D:\Apps\Yandex Messenger"

.NOTES
    Recommended GPO deployment:

    1. Copy the script to a read-only SYSVOL path. Grant ordinary users Read and
       Execute only, and restrict modification rights to administrators.
    2. Add the User example as a User Configuration logon script. Link that GPO
       to all target user OUs. This processes every targeted user at sign-in.
    3. Add the Machine example as a Computer Configuration startup script. Link
       that GPO to all target computer OUs.
    4. Prefer an organization-approved execution policy or code-sign the script.
       ExecutionPolicy Bypass is shown only for legacy deployment environments.
    5. Enable "Always wait for the network at computer startup and logon" where
       SYSVOL is not reliably available during early startup or sign-in.
    6. A user logon script cannot safely impersonate dormant profile owners. A
       user who never signs in after deployment will not have a per-user client
       uninstalled. Handle stale profiles through an approved profile lifecycle
       policy rather than loading and modifying user hives as LocalSystem.
#>

[CmdletBinding()]
param(
    [ValidateSet('Auto', 'User', 'Machine')]
    [string]$Context = 'Auto',

    [string]$Clients = '',

    [string]$LogDirectory = '',

    [ValidateRange(30, 86400)]
    [int]$ProcessTimeoutSeconds = 600,

    [ValidateRange(0, 10)]
    [int]$MsiRetryCount = 2,

    [ValidateRange(1, 3600)]
    [int]$MsiRetryDelaySeconds = 30,

    # Set these defaults here or pass the paths as command-line parameters.
    # Each path must be the directory containing .installInfo.json.
    [string]$UserMsiInstallPath = '',

    [string]$MachineMsiInstallPath = ''
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

$script:ExitCodeSuccess = 0
$script:ExitCodeNotInstalled = 1605
$script:ExitCodeInstallerBusy = 1618
$script:ExitCodeProductUninstalled = 1614
$script:ExitCodeRestartInitiated = 1641
$script:ExitCodeRestartRequired = 3010
$script:LogFile = $null

function Test-IsLocalSystem {
    try {
        $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
        return ($identity.User.Value -eq 'S-1-5-18')
    }
    catch {
        return ($env:USERNAME -eq 'SYSTEM')
    }
}

function Resolve-ExecutionContext {
    param([string]$RequestedContext)

    if ($RequestedContext -ne 'Auto') {
        return $RequestedContext
    }

    if (Test-IsLocalSystem) {
        return 'Machine'
    }

    return 'User'
}

function Get-NativeProgramFilesPath {
    # ProgramW6432 points to native 64-bit Program Files from a 32-bit process.
    if (-not [string]::IsNullOrEmpty($env:ProgramW6432)) {
        return $env:ProgramW6432
    }

    return $env:ProgramFiles
}

function Get-NativeMsiExecPath {
    $system32Path = Join-Path $env:SystemRoot 'System32\msiexec.exe'

    # Sysnative bypasses WOW64 file-system redirection for a 32-bit process.
    if (($env:PROCESSOR_ARCHITEW6432) -and
        (Test-Path -LiteralPath (Join-Path $env:SystemRoot 'Sysnative\msiexec.exe') -PathType Leaf)) {
        return (Join-Path $env:SystemRoot 'Sysnative\msiexec.exe')
    }

    return $system32Path
}

function Initialize-Log {
    param(
        [string]$RemovalContext,
        [string]$RequestedDirectory
    )

    $script:LogFile = $null
    $candidateDirectories = New-Object System.Collections.ArrayList

    if (-not [string]::IsNullOrEmpty($RequestedDirectory)) {
        [void]$candidateDirectories.Add($RequestedDirectory)
    }
    elseif ($RemovalContext -eq 'Machine') {
        [void]$candidateDirectories.Add((Join-Path $env:ProgramData 'YandexMessengerRemoval\Logs'))
    }
    else {
        [void]$candidateDirectories.Add((Join-Path $env:LOCALAPPDATA 'YandexMessengerRemoval\Logs'))
    }

    [void]$candidateDirectories.Add((Join-Path ([System.IO.Path]::GetTempPath()) 'YandexMessengerRemoval\Logs'))

    foreach ($directory in $candidateDirectories) {
        try {
            if (-not (Test-Path -LiteralPath $directory -PathType Container)) {
                [void](New-Item -Path $directory -ItemType Directory -Force)
            }

            $safeUserName = ($env:USERNAME -replace '[^A-Za-z0-9_.-]', '_')
            $fileName = 'remove-yandex-messenger-{0}-{1}.log' -f $env:COMPUTERNAME, $safeUserName
            $candidateLogFile = Join-Path $directory $fileName

            # Verify write access to the actual log file before selecting it.
            # Append mode preserves an existing log and creates a missing one.
            $logStream = [System.IO.File]::Open(
                $candidateLogFile,
                [System.IO.FileMode]::Append,
                [System.IO.FileAccess]::Write,
                [System.IO.FileShare]::ReadWrite
            )
            $logStream.Close()
            $script:LogFile = $candidateLogFile
            return
        }
        catch {
            # Try the next candidate. Logging must never prevent removal.
        }
    }
}

function Write-Log {
    param(
        [ValidateSet('INFO', 'WARN', 'ERROR')]
        [string]$Level,
        [string]$Message
    )

    $line = '{0} [{1}] {2}' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Level, $Message

    try {
        if (-not [string]::IsNullOrEmpty($script:LogFile)) {
            [System.IO.File]::AppendAllText(
                $script:LogFile,
                $line + [Environment]::NewLine,
                [System.Text.Encoding]::UTF8
            )
        }
    }
    catch {
        # Do not fail an uninstall because the log became unavailable.
    }
}

function Get-SelectedClients {
    param(
        [string]$ClientList,
        [string]$RemovalContext
    )

    if ([string]::IsNullOrEmpty($ClientList)) {
        if ($RemovalContext -eq 'Machine') {
            return @('LocalMachineMsi')
        }

        return @('UserExe', 'CurrentUserMsi')
    }

    $result = New-Object System.Collections.ArrayList
    $validClients = @('UserExe', 'CurrentUserMsi', 'LocalMachineMsi')

    foreach ($requestedClient in $ClientList.Split(',')) {
        $client = $requestedClient.Trim()

        if ([string]::IsNullOrEmpty($client)) {
            continue
        }

        if ($client -ieq 'All') {
            foreach ($validClient in $validClients) {
                if (-not $result.Contains($validClient)) {
                    [void]$result.Add($validClient)
                }
            }
            continue
        }

        $canonicalClient = $null
        foreach ($validClient in $validClients) {
            if ($client -ieq $validClient) {
                $canonicalClient = $validClient
                break
            }
        }

        if ($null -eq $canonicalClient) {
            Write-Log -Level 'WARN' -Message ('Unknown client identifier "{0}" was ignored.' -f $client)
            continue
        }

        if (-not $result.Contains($canonicalClient)) {
            [void]$result.Add($canonicalClient)
        }
    }

    return @($result.ToArray())
}

function Get-MsiProductCode {
    param([string]$InstallInfoPath)

    if (-not (Test-Path -LiteralPath $InstallInfoPath -PathType Leaf)) {
        Write-Log -Level 'INFO' -Message ('Install information file was not found: {0}' -f $InstallInfoPath)
        return $null
    }

    try {
        $json = [System.IO.File]::ReadAllText($InstallInfoPath)
        $match = [regex]::Match(
            $json,
            '"productCode"\s*:\s*"(?<code>[^"\\]+)"',
            [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
        )

        if (-not $match.Success) {
            Write-Log -Level 'WARN' -Message ('The productCode property was not found in {0}.' -f $InstallInfoPath)
            return $null
        }

        $productCode = $match.Groups['code'].Value.Trim()
        $guidPattern = '^\{?[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\}?$'

        if ($productCode -notmatch $guidPattern) {
            Write-Log -Level 'WARN' -Message ('An invalid productCode was found in {0}: {1}' -f $InstallInfoPath, $productCode)
            return $null
        }

        $guidText = $productCode.Trim('{}')
        $guidValue = [Guid]$guidText
        return ('{{{0}}}' -f $guidValue.ToString().ToUpperInvariant())
    }
    catch {
        Write-Log -Level 'ERROR' -Message ('Failed to read productCode from {0}. {1}' -f $InstallInfoPath, $_.Exception.Message)
        return $null
    }
}

function Invoke-ExternalUninstall {
    param(
        [string]$FilePath,
        [string]$Arguments,
        [string]$DisplayName,
        [int[]]$SuccessfulExitCodes,
        [int]$TimeoutSeconds,
        [int]$RetryCount = 0,
        [int]$RetryDelaySeconds = 1,
        [int[]]$RetryExitCodes = @()
    )

    $attempt = 0

    while ($attempt -le $RetryCount) {
        $attempt++

        try {
            Write-Log -Level 'INFO' -Message ('Starting removal of {0}. Attempt {1} of {2}.' -f $DisplayName, $attempt, ($RetryCount + 1))
            $process = Start-Process `
                -FilePath $FilePath `
                -ArgumentList $Arguments `
                -PassThru `
                -WindowStyle Hidden `
                -ErrorAction Stop

            $completed = $process.WaitForExit($TimeoutSeconds * 1000)
            if (-not $completed) {
                try {
                    $process.Kill()
                }
                catch {
                    # The process may have exited between the timeout and Kill.
                }

                Write-Log -Level 'ERROR' -Message ('Removal of {0} exceeded the {1}-second timeout; processing will continue.' -f $DisplayName, $TimeoutSeconds)
                return $false
            }

            $exitCode = $process.ExitCode

            if ($SuccessfulExitCodes -contains $exitCode) {
                if (($exitCode -eq $script:ExitCodeRestartInitiated) -or
                    ($exitCode -eq $script:ExitCodeRestartRequired)) {
                    Write-Log -Level 'WARN' -Message ('Removal of {0} succeeded and requested a restart. Exit code: {1}.' -f $DisplayName, $exitCode)
                }
                else {
                    Write-Log -Level 'INFO' -Message ('Removal of {0} completed successfully. Exit code: {1}.' -f $DisplayName, $exitCode)
                }
                return $true
            }

            if (($RetryExitCodes -contains $exitCode) -and ($attempt -le $RetryCount)) {
                Write-Log -Level 'WARN' -Message ('Removal of {0} returned retryable exit code {1}. Retrying in {2} seconds.' -f $DisplayName, $exitCode, $RetryDelaySeconds)
                Start-Sleep -Seconds $RetryDelaySeconds
                continue
            }

            Write-Log -Level 'ERROR' -Message ('Removal of {0} returned exit code {1}; processing will continue.' -f $DisplayName, $exitCode)
            return $false
        }
        catch {
            Write-Log -Level 'ERROR' -Message ('Removal of {0} failed; processing will continue. {1}' -f $DisplayName, $_.Exception.Message)
            return $false
        }
    }

    return $false
}

function Remove-UserExeClient {
    $uninstallerPath = Join-Path $env:LOCALAPPDATA 'Programs\chats\Uninstall Yandex Messenger.exe'

    if (-not (Test-Path -LiteralPath $uninstallerPath -PathType Leaf)) {
        Write-Log -Level 'INFO' -Message ('Per-user EXE uninstaller was not found: {0}' -f $uninstallerPath)
        return
    }

    [void](Invoke-ExternalUninstall `
        -FilePath $uninstallerPath `
        -Arguments '/S' `
        -DisplayName 'per-user EXE client' `
        -SuccessfulExitCodes @($script:ExitCodeSuccess) `
        -TimeoutSeconds $ProcessTimeoutSeconds)
}

function Remove-MsiClient {
    param(
        [string]$InstallInfoPath,
        [string]$DisplayName
    )

    $productCode = Get-MsiProductCode -InstallInfoPath $InstallInfoPath
    if ([string]::IsNullOrEmpty($productCode)) {
        return
    }

    $msiexecPath = Get-NativeMsiExecPath
    $arguments = '/x {0} /qn /norestart' -f $productCode
    $successfulExitCodes = @(
        $script:ExitCodeSuccess,
        $script:ExitCodeNotInstalled,
        $script:ExitCodeProductUninstalled,
        $script:ExitCodeRestartInitiated,
        $script:ExitCodeRestartRequired
    )

    [void](Invoke-ExternalUninstall `
        -FilePath $msiexecPath `
        -Arguments $arguments `
        -DisplayName $DisplayName `
        -SuccessfulExitCodes $successfulExitCodes `
        -TimeoutSeconds $ProcessTimeoutSeconds `
        -RetryCount $MsiRetryCount `
        -RetryDelaySeconds $MsiRetryDelaySeconds `
        -RetryExitCodes @($script:ExitCodeInstallerBusy))
}

function Invoke-RemovalPlan {
    param(
        [string]$RemovalContext,
        [string[]]$SelectedClients
    )

    foreach ($client in $SelectedClients) {
        try {
            switch ($client) {
                'UserExe' {
                    if ($RemovalContext -ne 'User') {
                        Write-Log -Level 'WARN' -Message 'UserExe was skipped because it must run in user context.'
                        break
                    }

                    Remove-UserExeClient
                    break
                }
                'CurrentUserMsi' {
                    if ($RemovalContext -ne 'User') {
                        Write-Log -Level 'WARN' -Message 'CurrentUserMsi was skipped because it must run in user context.'
                        break
                    }

                    $installDirectory = $UserMsiInstallPath
                    if ([string]::IsNullOrEmpty($installDirectory.Trim())) {
                        $installDirectory = Join-Path $env:LOCALAPPDATA 'Programs\Yandex Messenger'
                    }
                    $installInfoPath = Join-Path $installDirectory '.installInfo.json'
                    Remove-MsiClient -InstallInfoPath $installInfoPath -DisplayName 'per-user MSI client'
                    break
                }
                'LocalMachineMsi' {
                    if ($RemovalContext -ne 'Machine') {
                        Write-Log -Level 'WARN' -Message 'LocalMachineMsi was skipped because it must run in machine context.'
                        break
                    }

                    $installDirectory = $MachineMsiInstallPath
                    if ([string]::IsNullOrEmpty($installDirectory.Trim())) {
                        $installDirectory = Join-Path (Get-NativeProgramFilesPath) 'Yandex Messenger'
                    }
                    $installInfoPath = Join-Path $installDirectory '.installInfo.json'
                    Remove-MsiClient -InstallInfoPath $installInfoPath -DisplayName 'per-machine MSI client'
                    break
                }
            }
        }
        catch {
            Write-Log -Level 'ERROR' -Message ('Unexpected error while processing {0}; processing will continue. {1}' -f $client, $_.Exception.Message)
        }
    }
}

try {
    $resolvedContext = Resolve-ExecutionContext -RequestedContext $Context
    Initialize-Log -RemovalContext $resolvedContext -RequestedDirectory $LogDirectory

    Write-Log -Level 'INFO' -Message ('Script started. Context={0}; Identity={1}\{2}; PowerShell={3}.' -f $resolvedContext, $env:USERDOMAIN, $env:USERNAME, $PSVersionTable.PSVersion)

    if (($resolvedContext -eq 'Machine') -and (-not (Test-IsLocalSystem))) {
        Write-Log -Level 'WARN' -Message 'Machine context is not running as LocalSystem. Administrative rights may be insufficient.'
    }

    # @() prevents PowerShell 2.0 from scalarizing a one-item function result.
    $selectedClients = @(Get-SelectedClients -ClientList $Clients -RemovalContext $resolvedContext)

    if (($resolvedContext -eq 'User') -and (Test-IsLocalSystem)) {
        Write-Log -Level 'ERROR' -Message 'User context was requested under LocalSystem. Per-user removal was skipped to avoid processing the LocalSystem profile.'
    }
    elseif ($selectedClients.Count -eq 0) {
        Write-Log -Level 'WARN' -Message 'No valid clients were selected; no removal was attempted.'
    }
    else {
        Write-Log -Level 'INFO' -Message ('Selected clients: {0}.' -f ($selectedClients -join ', '))
        Invoke-RemovalPlan -RemovalContext $resolvedContext -SelectedClients $selectedClients
    }

    Write-Log -Level 'INFO' -Message 'Script completed.'
}
catch {
    Write-Log -Level 'ERROR' -Message ('A top-level error was suppressed to keep GPO processing healthy. {0}' -f $_.Exception.Message)
}

# GPO processing must not be marked as failed because a client is absent or broken.
exit 0
