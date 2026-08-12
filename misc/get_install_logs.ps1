<#
.SYNOPSIS
    Collect every trace of a Yandex Telemost EXE installation to determine its
    source (SCCM, Group Policy, WinGet, auto-updater, Yandex Disk, other software).

.DESCRIPTION
    Read-only forensic collector. Does not install, remove or change anything.
    Focus: EXE installer (NOT MSI).

    Collects:
      - Registry uninstall keys for HKLM + EVERY user hive (Telemost EXE installs
        per-user, so HKCU of the running user is not enough)
      - Windows event logs (Application, System, Security 4688, Sysmon, TaskScheduler,
        PowerShell, WMI-Activity) - telemost mentions only
      - Execution traces for manual EXE installs (no events): BAM (per-exe last
        run), UserAssist (GUI-launched exe + count)
      - SCCM / ConfigMgr logs (C:\Windows\CCM\Logs)
      - Group Policy logs (C:\Windows\Logs\GPLogs, setupapi*.log)
      - Scheduled Tasks (via COM Schedule.Service) including auto-updaters
      - WinGet / Appx / Microsoft Store history
      - Prefetch records (when the installer ran)
      - Windows Update / WSUS reporting
      - Application logs of Telemost and Yandex Disk from %LOCALAPPDATA%
        (Disk is itself a plausible installation source for Telemost)

.NOTES
    TWO RUN MODES (-Mode):
      Admin  - full collection. Requires elevation; aborts if not elevated so it
               can never produce a half-empty report that looks complete.
               Adds: BAM, Prefetch, Security log, Sysmon, other users' registry
               hives (incl. mounting NTUSER.DAT), SCCM logs, audit-policy state.
      User   - everything a standard user can legitimately read: HKLM + own HKCU
               uninstall keys, own UserAssist, Application/System/Setup/
               TaskScheduler/PowerShell logs, scheduled tasks, WinGet/Appx,
               Windows Update history. Admin-only sources are reported as
               SKIPPED, never as "not found".
      Auto   - default: Admin when elevated, User otherwise.

    Compatible with Windows PowerShell 3.0 and newer (tested intent for <5.1).
    Replacements for 5.1-only constructs:
      - Get-ScheduledTask  -> COM Schedule.Service
      - Export-Csv -Encoding -> dropped (pre-5.1 has no -Encoding; defaults to ASCII)
      - Get-CimInstance     -> Get-WmiObject (not used for MSI here)
      - Get-AppxPackage    -> wrapped in try/catch (PS 4.0+ / Win8+ only)

    MATCHING SCOPE: only Telemost entities. Matching is done against -Keyword and
    -Aliases exclusively - never against a bare 'yandex', which would report Yandex
    Browser, Disk, Updater and Music as "findings".

.OUTPUTS
    Folder Telemost_install_forensics_<timestamp>\ with:
      - summary_report.txt  - human-readable report + source hypothesis
      - *.csv               - one file per artifact source (Telemost matches only)
      - software_logs\      - verbatim copies of third-party software logs that
                              mention Telemost (SCCM, GPO, setupapi, Panther, WU)
                              plus the Application log .evtx dump
      - app_logs\<user>\telemost\ - %LOCALAPPDATA%\Yandex\Yandex.Telemost\logs
      - app_logs\<user>\disk\     - %LOCALAPPDATA%\Yandex\Yandex.Disk.2 (*.log*)
                              Copied verbatim (not keyword-filtered); the Telemost
                              mentions found inside them are indexed separately in
                              10b_app_log_telemost_mentions.csv

.EXAMPLE
    .\get_logs.ps1
    Auto mode: full collection when started elevated, user-scope collection otherwise.

.EXAMPLE
    .\get_logs.ps1 -Mode User -OutDir $env:TEMP\tm
    Runs under the logged-on user's own rights (helpdesk scenario, no admin rights
    needed). Registry is never modified - no hive mounting.

.EXAMPLE
    .\get_logs.ps1 -Mode Admin -DaysBack 90
    Full collection, event-log fallback scan bounded to the last 90 days.
#>

#Requires -Version 3.0
[CmdletBinding()]
param(
    # Admin = full collection (requires elevation), User = standard-user sources only,
    # Auto = pick by actual privileges. See .NOTES.
    [ValidateSet('Auto', 'Admin', 'User')]
    [string]$Mode = 'Auto',
    # Primary product keyword. Everything is matched against this and -Aliases ONLY:
    # a bare 'yandex' match would drag in Browser, Disk, Updater and every other
    # Yandex product, which is noise, not evidence.
    [string]$Keyword = 'telemost',
    # Extra product-name search strings (case-insensitive, ASCII only)
    [string[]]$Aliases = @('yandex.telemost', 'yandextelemost', 'yandex_telemost'),
    # Also list generic installer traces (SETUP/INSTALL/UPDATE/MSIEXEC) that are not
    # named after Telemost. Useful for time correlation, noisy by definition - off.
    [switch]$IncludeGenericInstallers,
    # Output folder (default: next to the script)
    [string]$OutDir,
    # Event-log lookback in days (0 = whole log). Bounds the slow fallback scan.
    [int]$DaysBack = 0,
    # Do not 'reg load' NTUSER.DAT of logged-off users (registry stays untouched)
    [switch]$SkipHiveLoad,
    # Per-file size cap when copying Telemost/Disk application logs. Guards against
    # dragging a multi-GB Disk log or a crash dump into the collection folder.
    [int]$MaxAppLogFileMB = 20
)

$ErrorActionPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'

# ---------- output folder ----------
if (-not $OutDir) {
    $OutDir = Join-Path $PSScriptRoot ("Telemost_install_forensics_{0}" -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
}
$null = New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
# Copied third-party software logs (SCCM, Group Policy, setupapi, Panther, WU) go
# to their own folder: they are bulky verbatim files, unlike the CSV/report the
# analyst actually reads first.
$LogDir = Join-Path $OutDir 'software_logs'
$null = New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
# Logs written by the Yandex applications themselves, one subfolder per user and
# per app: app_logs\<user>\telemost\ and app_logs\<user>\disk\
$AppLogRoot = Join-Path $OutDir 'app_logs'
$null = New-Item -ItemType Directory -Path $AppLogRoot -Force | Out-Null

function Write-Step($msg) {
    Write-Host "[*] $msg" -ForegroundColor Cyan
}
function Save-Csv($name, $rows) {
    $p = Join-Path $OutDir $name
    if ($rows -and @($rows).Count -gt 0) {
        # No -Encoding: pre-5.1 Export-Csv has no -Encoding param; default is ASCII.
        $rows | Select-Object * | Export-Csv -Path $p -NoTypeInformation
    } else {
        'no rows' | Out-File -FilePath $p -Encoding ASCII
    }
    return $p
}
function Save-Text($name, $content) {
    $p = Join-Path $OutDir $name
    if ($null -eq $content) { $content = '' }
    $content | Out-File -FilePath $p -Encoding ASCII
    return $p
}
# Copy a software log into software_logs\, keeping its origin in the file name:
# several collected logs share a leaf name (setupapi.app.log, Panther\*.log) and
# would otherwise overwrite each other.
function Copy-Artifact($fullPath) {
    try {
        $dir  = Split-Path $fullPath -Parent
        $leaf = Split-Path $fullPath -Leaf
        $tag  = ($dir -replace '[:\\/ ]', '_') -replace '^_+', ''
        if ($tag.Length -gt 60) { $tag = $tag.Substring($tag.Length - 60) }
        Copy-Item $fullPath -Destination (Join-Path $LogDir ("{0}__{1}" -f $tag, $leaf)) -Force -ErrorAction SilentlyContinue
    } catch {}
}

$summary = New-Object System.Collections.ArrayList
# Sources deliberately not queried in the current mode. Kept separate from "found
# nothing" so the report can never imply a source was checked when it was not.
$skippedSources = New-Object System.Collections.ArrayList
function Add-Skipped($source, $why) {
    [void]$skippedSources.Add("$source - $why")
}
function Add-Summary($section, $text) {
    [void]$summary.Add('')
    [void]$summary.Add("==== $section ====")
    if ($text -is [string]) {
        [void]$summary.Add($text)
    } else {
        foreach ($t in $text) { [void]$summary.Add($t) }
    }
}

# ---------- match pattern ----------
# Single source of truth for "is this entity Telemost?". Deliberately does NOT
# contain 'yandex' on its own: that turned every Yandex Browser task, Disk folder
# and Updater binary on the host into a false positive.
$aliasPattern = (@($Keyword) + $Aliases |
    Where-Object { $_ } |
    ForEach-Object { [regex]::Escape($_) } |
    Select-Object -Unique) -join '|'

# ---------- elevation ----------
# Half of the sources below are admin-only. Without admin they return nothing,
# which reads as "Telemost absent" instead of "could not look" - so state it loudly.
$isAdmin = $false
try {
    $wi = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $wp = New-Object System.Security.Principal.WindowsPrincipal($wi)
    $isAdmin = $wp.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)
} catch {}

# ---------- run mode ----------
# Admin mode aborts instead of degrading: a silently downgraded run produces a
# report whose "not found" lines are indistinguishable from "could not read".
$runMode = $Mode
if ($runMode -eq 'Auto') { $runMode = if ($isAdmin) { 'Admin' } else { 'User' } }
if ($runMode -eq 'Admin' -and -not $isAdmin) {
    Write-Host "-Mode Admin requires an elevated session." -ForegroundColor Red
    Write-Host "Start PowerShell as administrator, or use -Mode User for the standard-user subset." -ForegroundColor Yellow
    exit 2
}
# Capability flags - every admin-only source is gated on these, never on $isAdmin
# directly, so 'Admin on a non-elevated host' cannot silently half-collect.
$canSystemSources = ($runMode -eq 'Admin')   # BAM, Prefetch, Security/Sysmon, SCCM, auditpol
$canOtherUsers    = ($runMode -eq 'Admin')   # other users' hives; NTUSER.DAT mounting

Write-Host "=== Telemost installation forensics (EXE focus) ===" -ForegroundColor Green
Write-Host "Output dir: $OutDir"
Write-Host "Host: $($env:COMPUTERNAME)  User: $($env:USERNAME)  OS: $([System.Environment]::OSVersion.VersionString)"
if ($runMode -eq 'Admin') {
    Write-Host "Mode: ADMIN (full collection, elevated)" -ForegroundColor Green
} else {
    Write-Host "Mode: USER (standard-user sources only)" -ForegroundColor Yellow
    Write-Host "      Skipped: BAM, Prefetch, Security/Sysmon logs, SCCM logs, other users' hives, audit policy." -ForegroundColor Yellow
    Write-Host "      Re-run elevated with -Mode Admin for those. Nothing below is proof of absence." -ForegroundColor Yellow
}
Add-Summary "HOST" @(
    "Computer: $($env:COMPUTERNAME)",
    "User: $($env:USERNAME)",
    "OS: $([System.Environment]::OSVersion.VersionString)",
    "PSVersion: $($PSVersionTable.PSVersion)",
    "Elevated: $(if ($isAdmin) {'YES'} else {'NO'})",
    "Run mode: $runMode$(if ($Mode -eq 'Auto') {' (auto-selected)'} else {' (explicit)'})",
    "Collected: $(Get-Date -Format 'o')",
    "Output dir: $OutDir",
    "Software logs: $LogDir",
    "Scope: EXE installer (MSI search disabled by request)",
    "Match pattern (Telemost entities only): $aliasPattern",
    "Generic installer traces included: $(if ($IncludeGenericInstallers) {'YES (-IncludeGenericInstallers)'} else {'no'})"
)

# ============================================================
# 0. User registry hives
#    A Telemost EXE install is PER-USER, so HKCU of whoever runs this script is
#    the wrong hive on a multi-profile machine. Enumerate every loaded HKEY_USERS
#    hive and (unless -SkipHiveLoad) temporarily mount NTUSER.DAT of logged-off
#    users. Everything mounted here is unmounted at the end of the script; if the
#    script is interrupted, clean up with: reg unload HKU\TL_<sid>
#    (mounted keys are always named TL_<sid>). Use -SkipHiveLoad to never mount.
# ============================================================
Write-Step "Enumerating user registry hives"
$mountedHives = New-Object System.Collections.ArrayList   # keys we loaded ourselves
$userHives    = New-Object System.Collections.ArrayList   # @{Sid; Root; Profile; Loaded}

$ownSid = ''
try { $ownSid = ([System.Security.Principal.WindowsIdentity]::GetCurrent()).User.Value } catch {}

$loadedSids = @()
try {
    Get-ChildItem 'Registry::HKEY_USERS' -ErrorAction SilentlyContinue | ForEach-Object {
        $n = $_.PSChildName
        if ($n -notmatch '^S-1-5-21-' -or $n -match '_Classes$') { return }
        # User mode: only the caller's own hive is actually readable, and reading
        # another user's hive is not something a helpdesk run should attempt.
        if (-not $canOtherUsers -and $n -ne $ownSid) { return }
        $loadedSids += $n
        [void]$userHives.Add([PSCustomObject]@{
            Sid = $n; Root = "Registry::HKEY_USERS\$n"; Profile = ''; Loaded = $true })
    }
} catch {}

if (-not $canOtherUsers) {
    Add-Skipped 'Other users registry hives' 'user mode (needs admin; only own HKCU collected)'
} elseif ($SkipHiveLoad) {
    Add-Skipped 'NTUSER.DAT of logged-off users' '-SkipHiveLoad requested'
}

if ($canOtherUsers -and -not $SkipHiveLoad) {
    $profileList = 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\ProfileList'
    Get-ChildItem $profileList -ErrorAction SilentlyContinue | ForEach-Object {
        $sid = $_.PSChildName
        if ($sid -notmatch '^S-1-5-21-' -or ($loadedSids -contains $sid)) { return }
        $profPath = (Get-ItemProperty $_.PSPath -Name ProfileImagePath -ErrorAction SilentlyContinue).ProfileImagePath
        if (-not $profPath) { return }
        $dat = Join-Path $profPath 'NTUSER.DAT'
        if (-not (Test-Path $dat)) { return }
        $mountKey = "TL_$sid"
        # reg.exe rather than a native API: works on PS 3.0 and needs no P/Invoke.
        $null = & reg.exe load "HKU\$mountKey" "$dat" 2>&1
        if ($LASTEXITCODE -eq 0) {
            [void]$mountedHives.Add($mountKey)
            [void]$userHives.Add([PSCustomObject]@{
                Sid = $sid; Root = "Registry::HKEY_USERS\$mountKey"; Profile = $profPath; Loaded = $false })
        }
    }
}
Write-Host ("  User hives available: {0} (loaded by us: {1})" -f @($userHives).Count, @($mountedHives).Count) -ForegroundColor Gray
$hiveScope = if ($canOtherUsers) { 'all profiles' } else { "own user only ($ownSid)" }
Add-Summary "USER HIVES" $(if (@($userHives).Count -gt 0) {
    @("Scope: $hiveScope") +
    ($userHives | ForEach-Object { "- SID=$($_.Sid) already-loaded=$($_.Loaded) profile=$($_.Profile)" })
} else { "No user hives enumerated (scope: $hiveScope)." })

# ============================================================
# 1. Registry: uninstall keys (version, date, source, uninstall string)
# ============================================================
Write-Step ("Collecting registry uninstall keys (HKLM + {0})" -f $(if ($canOtherUsers) { 'every user hive' } else { 'own user' }))
$uninstallKeys = @(
    'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*',
    'HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*',
)
# Own hive via HKCU only if it was not already picked up through HKEY_USERS -
# otherwise every per-user entry would be listed twice.
if (-not ($userHives | Where-Object { $_.Sid -eq $ownSid })) {
    $uninstallKeys += 'HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*'
    $uninstallKeys += 'HKCU:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*'
}
foreach ($h in $userHives) {
    $uninstallKeys += "$($h.Root)\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*"
    $uninstallKeys += "$($h.Root)\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*"
}
$regHits = foreach ($k in $uninstallKeys) {
    Get-ItemProperty $k -ErrorAction SilentlyContinue |
        Where-Object {
            # Publisher='Yandex' is NOT a criterion: it matches Browser, Disk and
            # every other Yandex product installed on the box.
            $_.DisplayName -match $aliasPattern -or
            $_.InstallLocation -match $aliasPattern -or
            $_.UninstallString -match $aliasPattern
        } |
        Select-Object DisplayName, DisplayVersion, Publisher, InstallDate,
                      InstallLocation, InstallSource, RegOwner, WindowsInstaller,
                      UninstallString, QuietUninstallString, PSPath
}
Save-Csv '01_registry_uninstall.csv' $regHits

if ($regHits) {
    Add-Summary "REGISTRY (uninstall keys)" ($regHits | ForEach-Object {
        "- $($_.DisplayName) v$($_.DisplayVersion) | publisher=$($_.Publisher) | date=$($_.InstallDate) | source=$($_.InstallSource) | loc=$($_.InstallLocation) | uninstall=$($_.UninstallString)"
    })
    Write-Host ("  Telemost uninstall entries found: {0}" -f @($regHits).Count) -ForegroundColor Yellow
} else {
    Add-Summary "REGISTRY (uninstall keys)" "Telemost NOT found in uninstall registry keys."
    Write-Host "  Telemost not found in uninstall keys" -ForegroundColor Red
}

# ============================================================
# 2. Windows Event Logs: Application, System, GroupPolicy
# ============================================================
Write-Step "Reading Windows event logs (telemost mentions only)"
# Broad log set: EXE installs rarely write to Application/MsiInstaller. Security 4688
# (process creation, if audited), Sysmon/Operational (if deployed), TaskScheduler
# (updater runs), PowerShell/WMI (script-launched installs) fill the gap.
$eventLogs = @(
    'Application',
    'System',
    'Setup',
    'Microsoft-Windows-TaskScheduler/Operational',
    'Microsoft-Windows-PowerShell/Operational',
    'Microsoft-Windows-WMI-Activity/Operational'
)
# Security / Sysmon / AppLocker are readable only by administrators: querying them
# as a standard user yields an access error that would be swallowed by the catch
# below and look exactly like "no matching events".
if ($canSystemSources) {
    $eventLogs += 'Security'
    $eventLogs += 'Microsoft-Windows-Sysmon/Operational'
    $eventLogs += 'Microsoft-Windows-AppLocker/EXE and DLL'
} else {
    Add-Skipped 'Event logs Security (4688) / Sysmon / AppLocker' 'user mode (admin-only read access)'
}

# Server-side XPath match first: rendering every message of 9 logs client-side is
# slow, and -MaxEvents 5000 on Security can cover barely an hour of history - an
# install from last month would simply fall outside the window.
# Two constraints of the Windows Event Log XPath subset shape the query below:
# contains() is case-sensitive (hence the case variants) and the '//' descendant
# axis and translate() are not supported (hence EventData/Data only).
$needles = @($Keyword.ToLower(), $Keyword.ToUpper(),
             ($Keyword.Substring(0,1).ToUpper() + $Keyword.Substring(1).ToLower())) |
           Select-Object -Unique
$xpTerms = ($needles | ForEach-Object { "contains(.,'$_')" }) -join ' or '
$xpath   = "*[EventData[Data[$xpTerms]]]"

$startTime = $null
if ($DaysBack -gt 0) { $startTime = (Get-Date).AddDays(-$DaysBack) }

$events = foreach ($log in $eventLogs) {
    $viaXPath = $null
    # Throws when the log is absent/empty or nothing matches - both mean "fall back".
    try { $viaXPath = Get-WinEvent -LogName $log -FilterXPath $xpath -MaxEvents 5000 -ErrorAction Stop } catch { $viaXPath = $null }
    if ($viaXPath) {
        $viaXPath
    } else {
        # Fallback: providers that keep the product name only in the localised message
        # template are invisible to XPath. Bounded client-side scan.
        try {
            if ($startTime) {
                Get-WinEvent -FilterHashtable @{ LogName = $log; StartTime = $startTime } -MaxEvents 5000 -ErrorAction Stop |
                    Where-Object { $_.Message -match $aliasPattern }
            } else {
                Get-WinEvent -LogName $log -MaxEvents 5000 -ErrorAction Stop |
                    Where-Object { $_.Message -match $aliasPattern }
            }
        } catch {}
    }
}
Save-Csv '02_events.csv' ($events | Select-Object TimeCreated, Id, LogName, ProviderName, LevelDisplayName, @{n='Msg';e={$_.Message -replace '\s+',' '}})

if ($events) {
    Add-Summary "EVENTS (telemost mentions only)" ($events | Sort-Object TimeCreated | Select-Object -First 20 | ForEach-Object {
        "- $($_.TimeCreated) [$($_.LogName)] $($_.ProviderName) ID=$($_.Id) $($_.Message -replace '\s+',' ')"
    })
    Write-Host ("  Events mentioning Telemost: {0}" -f @($events).Count) -ForegroundColor Yellow
} else {
    Add-Summary "EVENTS (telemost mentions only)" "No events mentioning Telemost found in the collected logs."
}

# --- Is process-creation auditing even on? ---
# "No 4688 events" proves nothing unless the audit policy was enabled BEFORE the
# install, so record the policy state next to the (possibly empty) result.
$auditNotes = @()
if ($canSystemSources) {
    try {
        $ap = & auditpol.exe /get /subcategory:"Process Creation" /r 2>$null
        $apLine = $ap | Where-Object { $_ -match 'Process Creation' }
        $auditNotes += "auditpol: $(if ($apLine) { ($apLine -join ' ') } else { 'could not parse output' })"
    } catch { $auditNotes += "auditpol: failed - $($_.Exception.Message)" }
} else {
    $auditNotes += "auditpol: skipped (user mode, needs admin)"
    Add-Skipped 'Audit policy state (auditpol)' 'user mode (needs admin)'
}
$cmdLineAudit = (Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Policies\System\Audit' -Name ProcessCreationIncludeCmdLine_Enabled -ErrorAction SilentlyContinue).ProcessCreationIncludeCmdLine_Enabled
$auditNotes += "ProcessCreationIncludeCmdLine_Enabled: $(if ($null -ne $cmdLineAudit) { $cmdLineAudit } else { 'not set (4688 events carry NO command line)' })"
$sysmonPresent = 'no'
try { if (Get-WinEvent -ListLog 'Microsoft-Windows-Sysmon/Operational' -ErrorAction Stop) { $sysmonPresent = 'yes' } } catch {}
$auditNotes += "Sysmon operational log present: $sysmonPresent"
Add-Summary "AUDIT POLICY (how trustworthy is an empty event result)" $auditNotes
Save-Text '02c_audit_policy.txt' ($auditNotes -join "`n")

# Dump Application log to evtx for offline analysis
try {
    $evtxPath = Join-Path $LogDir '02_application.evtx'
    $null = Start-Process -FilePath 'wevtutil' -ArgumentList @('epl','Application',$evtxPath) -Wait -PassThru -WindowStyle Hidden -ErrorAction SilentlyContinue
    if (Test-Path $evtxPath) { Add-Summary "EVENTS (evtx dump)" "Application log dumped to software_logs\02_application.evtx for offline analysis." }
} catch {}

# ============================================================
# 2b. Execution traces (non-event sources)
#     Finds the install / run time even when an EXE installer leaves no
#     MsiInstaller/Application event (e.g. a manual double-click install).
#     Sources:
#       - BAM  (Background Activity Moderator) per-exe last run, Win10 1709+
#       - UserAssist (GUI-launched exe last run + count, per user)
# ============================================================
Write-Step "Collecting execution traces (BAM, UserAssist)"

function ConvertFrom-Rot13([string]$s) {
    $sb = New-Object System.Text.StringBuilder
    foreach ($c in $s.ToCharArray()) {
        $o = [int][char]$c
        if ($o -ge 65 -and $o -le 90) { $o = 65 + (($o - 65 + 13) % 26) }
        elseif ($o -ge 97 -and $o -le 122) { $o = 97 + (($o - 97 + 13) % 26) }
        [void]$sb.Append([char]$o)
    }
    return $sb.ToString()
}
# Read a FILETIME at a KNOWN offset. Scanning offsets until something "looks like"
# a date is how you end up reporting a float from the UserAssist blob as an install
# time - both structures below have fixed, documented layouts.
function Get-FileTimeAt($bytes, [int]$offset) {
    if ($null -eq $bytes) { return $null }
    if ($bytes.Length -lt ($offset + 8)) { return $null }
    try {
        $ft = [BitConverter]::ToInt64($bytes, $offset)
        if ($ft -le 0) { return $null }
        $dt = [DateTime]::FromFileTimeUtc($ft).ToLocalTime()
        if ($dt.Year -ge 2008 -and $dt.Year -le 2035) { return $dt }
    } catch {}
    return $null
}

$execTraces = New-Object System.Collections.ArrayList

# --- BAM (Background Activity Moderator) - per-exe last run, Win10 1709+ ---
# Value = 24 bytes, last execution FILETIME at offset 0.
# Key moved in 1809: bam\UserSettings (1709) -> bam\State\UserSettings (1809+).
$bamBases = @()
if ($canSystemSources) {
    $bamBases = @(
        'HKLM:\SYSTEM\CurrentControlSet\Services\bam\State\UserSettings',
        'HKLM:\SYSTEM\CurrentControlSet\Services\bam\UserSettings'
    ) | Where-Object { Test-Path $_ }
} else {
    Add-Skipped 'BAM (last run per exe)' 'user mode (HKLM\SYSTEM\...\bam is admin-only)'
}
$bamRows = @()
foreach ($bamBase in $bamBases) {
    $bamUserKeys = Get-ChildItem $bamBase -ErrorAction SilentlyContinue
    foreach ($uk in $bamUserKeys) {
        $sid = $uk.PSChildName
        foreach ($valName in $uk.Property) {
            $path = $valName
            if ($path -match $aliasPattern) {
                $bytes = $uk.GetValue($valName)
                $dt = Get-FileTimeAt $bytes 0
                $bamRows += [PSCustomObject]@{ Source = 'BAM'; Path = $path; Timestamp = $dt; Extra = "SID=$sid; key=$bamBase" }
            }
        }
    }
}
if (-not $canSystemSources) {
    Add-Summary "BAM (last run per exe)" "SKIPPED - user mode. HKLM\SYSTEM\...\bam needs admin; this is not evidence of absence."
    Write-Host "  BAM: skipped (user mode)" -ForegroundColor Gray
} elseif ($bamRows) {
    Save-Csv '02b_bam.csv' $bamRows
    $bamRows | ForEach-Object { [void]$execTraces.Add($_) }
    Add-Summary "BAM (last run per exe)" ($bamRows | ForEach-Object { "- $($_.Timestamp)  $($_.Path)  [$($_.Extra)]" })
    Write-Host ("  BAM entries: {0}" -f @($bamRows).Count) -ForegroundColor Yellow
} else {
    Add-Summary "BAM (last run per exe)" "No Telemost entries in BAM (BAM is Win10 1709+ and may not track this process)."
}

# --- UserAssist (GUI-launched exe last run + count, per user) ---
# Win7+ value layout (72 bytes): 0x00 session, 0x04 run count,
# 0x08 focus count, 0x0C focus time, 0x10..0x3B floats, 0x3C last-run FILETIME.
$uaRows = @()
foreach ($h in $userHives) {
    $uaBase = "$($h.Root)\Software\Microsoft\Windows\CurrentVersion\Explorer\UserAssist"
    if (-not (Test-Path $uaBase)) { continue }
    $guidKeys = Get-ChildItem $uaBase -ErrorAction SilentlyContinue
    foreach ($gk in $guidKeys) {
        foreach ($valName in $gk.Property) {
            $decoded = ConvertFrom-Rot13 $valName
            if ($decoded -match $aliasPattern) {
                $bytes = $gk.GetValue($valName)
                $dt = Get-FileTimeAt $bytes 60
                $rc = $null
                try { if ($bytes -and $bytes.Length -ge 8) { $rc = [BitConverter]::ToUInt32($bytes, 4) } } catch {}
                $uaRows += [PSCustomObject]@{
                    Source = 'UserAssist'; Path = $decoded; Timestamp = $dt
                    Extra = "count=$rc; SID=$($h.Sid)"
                }
            }
        }
    }
}
if ($uaRows) {
    Save-Csv '02b_userassist.csv' $uaRows
    $uaRows | ForEach-Object { [void]$execTraces.Add($_) }
    Add-Summary "USERASSIST (GUI-launched exe)" ($uaRows | ForEach-Object { "- $($_.Timestamp)  $($_.Path)  [$($_.Extra)]" })
    Write-Host ("  UserAssist entries: {0}" -f @($uaRows).Count) -ForegroundColor Yellow
} else {
    Add-Summary "USERASSIST (GUI-launched exe)" "No Telemost entries in UserAssist (install may not have been GUI-launched, or hives not loaded)."
}

Save-Csv '02b_execution_traces.csv' $execTraces

# ============================================================
# 3. SCCM / Microsoft Configuration Manager logs
# ============================================================
Write-Step "Checking SCCM / ConfigMgr"
$sccmLogDir = "$env:WINDIR\CCM\Logs"
$sccmFindings = @()
if (-not $canSystemSources) {
    Add-Skipped 'SCCM / ConfigMgr logs' 'user mode (C:\Windows\CCM\Logs is admin-only)'
    Add-Summary "SCCM (ConfigMgr)" "SKIPPED - user mode. C:\Windows\CCM\Logs is not readable by standard users, so SCCM deployment can be neither confirmed nor ruled out here."
    Write-Host "  SCCM logs: skipped (user mode)" -ForegroundColor Gray
} elseif (Test-Path $sccmLogDir) {
    $sccmTargetLogs = @(
        'AppIntentEval.log', 'AppDiscovery.log', 'AppEnforce.log',
        'CAS.log', 'ContentTransferManager.log', 'DataTransferService.log',
        'ExecMandatoryTask.log', 'TaskSequence.log', 'CcmExec.log',
        'CIAgent.log', 'CIStore.log', 'CIStateMessage.log', 'StateMessage.log',
        'AppISV.log', 'DCMAgent.log'
    )
    $sccmCopied = 0
    foreach ($tl in $sccmTargetLogs) {
        $p = Join-Path $sccmLogDir $tl
        if (Test-Path $p) {
            try {
                $hit = Select-String -Path $p -Pattern $aliasPattern -List -ErrorAction SilentlyContinue
                if ($hit) {
                    $sccmFindings += "$p : $($hit.Line)"
                    Copy-Artifact $p
                    $sccmCopied++
                }
            } catch {}
        }
    }
    Add-Summary "SCCM (C:\Windows\CCM\Logs)" $(if ($sccmFindings) { $sccmFindings } else { "SCCM folder exists but no Telemost mentions in target logs." })
    Write-Host ("  SCCM logs with Telemost: {0} (copied: {1})" -f @($sccmFindings).Count, $sccmCopied) -ForegroundColor $(if ($sccmFindings) {'Yellow'} else {'Gray'})
} else {
    Add-Summary "SCCM (ConfigMgr)" "C:\Windows\CCM\Logs not found - SCCM client most likely not installed."
    Write-Host "  SCCM client not installed (no C:\Windows\CCM\Logs)" -ForegroundColor Gray
}

# ============================================================
# 4. Group Policy: logs + setupapi
# ============================================================
Write-Step "Checking Group Policy"
$gpPaths = @(
    "$env:WINDIR\Logs\GPLogs",
    "$env:WINDIR\inf\setupapi.dev.log",
    "$env:WINDIR\inf\setupapi.app.log",
    "$env:WINDIR\Panther",
    "$env:WINDIR\Logs\WindowsUpdate",
    "$env:WINDIR\SoftwareDistribution\ReportingEvents.log"
)
$gpFindings = @()
foreach ($p in $gpPaths) {
    if (-not (Test-Path $p)) { continue }
    if (Test-Path $p -PathType Container) {
        Get-ChildItem -Path $p -Recurse -File -ErrorAction SilentlyContinue | ForEach-Object {
            try {
                $hit = Select-String -Path $_.FullName -Pattern $aliasPattern -List -ErrorAction SilentlyContinue
                if ($hit) {
                    $gpFindings += "$($_.FullName) : $($hit.Line)"
                    Copy-Artifact $_.FullName
                }
            } catch {}
        }
    } else {
        try {
            $hit = Select-String -Path $p -Pattern $aliasPattern -List -ErrorAction SilentlyContinue
            if ($hit) {
                $gpFindings += "$p : $($hit.Line)"
                Copy-Artifact $p
            }
        } catch {}
    }
}
# GroupPolicy operational event log (last 60 days)
$gpResult = $null
try {
    $gpResult = Get-WinEvent -FilterHashtable @{LogName='Microsoft-Windows-GroupPolicy/Operational'; StartTime=(Get-Date).AddDays(-60)} -MaxEvents 2000 -ErrorAction Stop |
        Where-Object { $_.Message -match $aliasPattern }
} catch {}
if ($gpResult) {
    $gpResult | ForEach-Object { $gpFindings += "GPO event $($_.TimeCreated) ID=$($_.Id): $($_.Message -replace '\s+',' ')" }
}
Add-Summary "GROUP POLICY" $(if ($gpFindings) { $gpFindings } else { "No direct Telemost mentions in GP/setupapi/WU logs." })
Write-Host ("  GP/setupapi/WU findings: {0}" -f @($gpFindings).Count) -ForegroundColor $(if ($gpFindings) {'Yellow'} else {'Gray'})

# ============================================================
# 5. Scheduled Tasks via COM Schedule.Service (PS 3.0+ compatible)
# ============================================================
Write-Step "Checking Scheduled Tasks"
$schedTasks = @()
try {
    $svc = New-Object -ComObject Schedule.Service
    $svc.Connect()
    $stateMap = @{0='Unknown';1='Disabled';2='Queued';3='Ready';4='Running'}

    function Get-SubTasks($folder) {
        $folder.GetTasks(0) | ForEach-Object {
            $task = $_
            $actionStr = ''
            try {
                $acts = @($task.Definition.Actions)
                $parts = @()
                foreach ($a in $acts) { $parts += ($a.Exec + ' ' + $a.Arguments) }
                $actionStr = ($parts -join '; ')
            } catch {}
            $st = $task.State
            try { $st = $stateMap[[int]$task.State] } catch {}
            [PSCustomObject]@{
                TaskName = $task.Name
                TaskPath = $task.Path
                State    = $st
                LastRun   = $task.LastRunTime
                Actions  = $actionStr
            }
        }
        $folder.GetFolders(0) | ForEach-Object { Get-SubTasks $_ }
    }
    $root = $svc.GetFolder('\')
    $allTasks = Get-SubTasks $root
    $schedTasks = $allTasks | Where-Object {
        # Not TaskPath -match 'yandex': \Yandex\ holds the Browser updater tasks too.
        $_.TaskName -match $aliasPattern -or
        $_.Actions -match $aliasPattern
    }
} catch {
    Add-Summary "SCHEDULED TASKS" "Failed to query via Schedule.Service: $($_.Exception.Message)"
}
Save-Csv '06_scheduled_tasks.csv' $schedTasks
if (-not $canSystemSources) {
    # Schedule.Service returns only tasks the caller may read, and it does so
    # without an error for the rest - so state the partial coverage explicitly.
    Add-Summary "SCHEDULED TASKS (coverage note)" "User mode: tasks whose ACL denies read access to standard users (typically SYSTEM-owned deployment/updater tasks) are silently absent from this list."
    Add-Skipped 'Scheduled tasks readable only by admins' 'user mode (partial task list collected)'
}
if ($schedTasks) {
    Add-Summary "SCHEDULED TASKS" ($schedTasks | ForEach-Object {
        "- $($_.TaskPath)$($_.TaskName) | state=$($_.State) | actions=$($_.Actions) | lastrun=$($_.LastRun)"
    })
    Write-Host ("  Scheduled tasks referencing Telemost: {0}" -f @($schedTasks).Count) -ForegroundColor Yellow
}

# ============================================================
# 7. WinGet / Appx / Microsoft Store
# ============================================================
Write-Step "Checking WinGet / Appx"
$winGet = $null
$wingetNote = $null
# $LASTEXITCODE -ne $null was always true; and winget blocks on source agreements
# when it has never run interactively in this profile (e.g. under SYSTEM).
if (Get-Command winget.exe -ErrorAction SilentlyContinue) {
    try {
        $wingetOut = & winget.exe list --accept-source-agreements --disable-interactivity 2>&1
        if ($LASTEXITCODE -eq 0) {
            $winGet = $wingetOut | Select-String -Pattern $aliasPattern
            if (-not $winGet) { $wingetNote = "winget ran, no Telemost package listed." }
        } else {
            $wingetNote = "winget exit code $LASTEXITCODE - output: $(($wingetOut | Select-Object -First 3) -join ' / ')"
        }
    } catch { $wingetNote = "winget call failed: $($_.Exception.Message)" }
} else {
    $wingetNote = "winget.exe not found in PATH (App Installer absent, or running in a context without the user's PATH)."
}
Add-Summary "WINGET" $(if ($winGet) { $winGet } else { $wingetNote })

$appx = $null
try {
    $appx = Get-AppxPackage -ErrorAction Stop |
        Where-Object { $_.Name -match $aliasPattern -or $_.PackageFullName -match $aliasPattern } |
        Select-Object Name, PackageFullName, InstallLocation, InstallDate, Publisher
} catch {
    # Get-AppxPackage unavailable (older PS / older Windows)
}
if ($appx) {
    Save-Csv '07_appx.csv' $appx
    Add-Summary "APPX/STORE" ($appx | ForEach-Object { "- $($_.PackageFullName) | loc=$($_.InstallLocation) | date=$($_.InstallDate)" })
}

# ============================================================
# 8. Prefetch - when the installer/updater ran
# ============================================================
Write-Step "Checking Prefetch"
$prefetchDir = "$env:WINDIR\Prefetch"
$prefetch = @()
$prefetchInstallers = @()
if (-not $canSystemSources) {
    Add-Skipped 'Prefetch' 'user mode (C:\Windows\Prefetch is admin-only)'
    Add-Summary "PREFETCH" "SKIPPED - user mode. C:\Windows\Prefetch needs admin."
    Write-Host "  Prefetch: skipped (user mode)" -ForegroundColor Gray
} elseif (Test-Path $prefetchDir) {
    $allPf = Get-ChildItem -Path $prefetchDir -Filter '*.pf' -ErrorAction SilentlyContinue |
        Select-Object Name, CreationTime, LastWriteTime, Length
    $prefetch = $allPf | Where-Object { $_.Name -match $aliasPattern }
    # The installer is sometimes not named after the product (SETUP.EXE, a random
    # temp name). Those records are mostly unrelated software, so they are opt-in.
    if ($IncludeGenericInstallers) {
        $prefetchInstallers = $allPf | Where-Object { $_.Name -match 'SETUP|INSTALL|UPDAT|MSIEXEC|PACKAGE' }
    }
} else {
    Add-Summary "PREFETCH" "C:\Windows\Prefetch does not exist (prefetching disabled on this host, e.g. SSD/VDI image)."
}
Save-Csv '08_prefetch.csv' $prefetch
if ($IncludeGenericInstallers) { Save-Csv '08b_prefetch_installers.csv' $prefetchInstallers }
if ($prefetch) {
    Add-Summary "PREFETCH" ($prefetch | ForEach-Object { "- $($_.Name) | created=$($_.CreationTime) | last=$($_.LastWriteTime)" })
    Write-Host ("  Telemost prefetch records: {0}" -f @($prefetch).Count) -ForegroundColor Yellow
}
if ($prefetchInstallers) {
    Add-Summary "PREFETCH (generic installers, correlate by time)" ($prefetchInstallers | Sort-Object LastWriteTime -Descending | Select-Object -First 25 | ForEach-Object {
        "- $($_.Name) | created=$($_.CreationTime) | last=$($_.LastWriteTime)"
    })
}

# ============================================================
# 9. Windows Update / WSUS history
# ============================================================
Write-Step "Checking Windows Update / BITS history"
$wuHistory = $null
try {
    $session = New-Object -ComObject Microsoft.Update.Session
    $searcher = $session.CreateUpdateSearcher()
    $wuHistory = $searcher.QueryHistory(0, 100) |
        Where-Object { $_.Title -match $aliasPattern -or $_.Description -match $aliasPattern } |
        Select-Object Date, Title, Description, ResultCode
} catch {}
if ($wuHistory) {
    Save-Csv '09_wu_history.csv' $wuHistory
    Add-Summary "WINDOWS UPDATE HISTORY" ($wuHistory | ForEach-Object { "- $($_.Date) | $($_.Title) | result=$($_.ResultCode)" })
}

# ============================================================
# 9b. Application logs (Telemost, Yandex Disk)
#     The apps' own logs are collected verbatim, NOT keyword-filtered: the point
#     is to keep the raw timeline of what the client did to itself (self-update,
#     reinstall, install triggered by Disk). Disk is included because it is one of
#     the plausible installation sources for Telemost.
# ============================================================
Write-Step "Collecting application logs (Telemost, Yandex Disk)"

# ScanMentions: grep the copied files for Telemost. Pointless for Telemost's own
# logs (every line would match); the signal is Telemost showing up in DISK's logs.
$appLogSources = @(
    [PSCustomObject]@{ App = 'telemost'; Relative = 'Yandex\Yandex.Telemost\logs'; Pattern = '*';       ScanMentions = $false },
    # Disk.2 also holds caches/db files, so only log-shaped files are taken.
    [PSCustomObject]@{ App = 'disk';     Relative = 'Yandex\Yandex.Disk.2';        Pattern = '*.log*';  ScanMentions = $true  }
)

# Which profiles to look at: own only in user mode, all real user profiles in admin mode.
$profileTargets = @()
if ($canOtherUsers) {
    Get-ChildItem 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\ProfileList' -ErrorAction SilentlyContinue | ForEach-Object {
        if ($_.PSChildName -notmatch '^S-1-5-21-') { return }
        $pp = (Get-ItemProperty $_.PSPath -Name ProfileImagePath -ErrorAction SilentlyContinue).ProfileImagePath
        if ($pp -and (Test-Path $pp)) {
            $profileTargets += [PSCustomObject]@{ User = (Split-Path $pp -Leaf); LocalAppData = (Join-Path $pp 'AppData\Local') }
        }
    }
} else {
    if ($env:LOCALAPPDATA) {
        $profileTargets += [PSCustomObject]@{ User = $env:USERNAME; LocalAppData = $env:LOCALAPPDATA }
    }
    Add-Skipped 'Application logs of other users (Telemost, Disk)' 'user mode (only own %LOCALAPPDATA% is readable)'
}

$appLogStats = @()
$appLogMentions = @()
foreach ($pt in $profileTargets) {
    $userTag = ($pt.User -replace '[^A-Za-z0-9._-]', '_')
    foreach ($src in $appLogSources) {
        $sourceDir = Join-Path $pt.LocalAppData $src.Relative
        if (-not (Test-Path $sourceDir)) {
            $appLogStats += [PSCustomObject]@{
                User = $pt.User; App = $src.App; Source = $sourceDir
                Files = 0; SkippedTooBig = 0; SizeKB = 0; Note = 'folder not present'
            }
            continue
        }
        $destDir = Join-Path (Join-Path $AppLogRoot $userTag) $src.App
        $null = New-Item -ItemType Directory -Path $destDir -Force | Out-Null

        $copied = 0; $tooBig = 0; $bytes = 0
        Get-ChildItem -Path $sourceDir -Recurse -File -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -like $src.Pattern } |
            ForEach-Object {
                if ($_.Length -gt ($MaxAppLogFileMB * 1MB)) { $tooBig++; return }
                # Keep the folder structure of the app's log dir - the subfolder a
                # log sits in is part of its meaning (per-session dirs, crash dumps).
                $rel = $_.FullName.Substring($sourceDir.Length).TrimStart('\')
                $target = Join-Path $destDir $rel
                $targetDir = Split-Path $target -Parent
                if (-not (Test-Path $targetDir)) { $null = New-Item -ItemType Directory -Path $targetDir -Force | Out-Null }
                # Live logs are held open by the app; copy is best-effort.
                Copy-Item $_.FullName -Destination $target -Force -ErrorAction SilentlyContinue
                if (Test-Path $target) { $copied++; $bytes += $_.Length }
            }

        $appLogStats += [PSCustomObject]@{
            User = $pt.User; App = $src.App; Source = $sourceDir
            Files = $copied; SkippedTooBig = $tooBig; SizeKB = [math]::Round($bytes / 1KB, 1)
            Note = $(if ($copied -eq 0) { 'folder present, nothing copied (empty, filtered out or locked)' } else { '' })
        }

        # Telemost mentions inside Disk's logs are direct evidence of who installed what.
        if ($copied -gt 0 -and $src.ScanMentions) {
            Get-ChildItem -Path $destDir -Recurse -File -ErrorAction SilentlyContinue | ForEach-Object {
                $hit = Select-String -Path $_.FullName -Pattern $aliasPattern -List -ErrorAction SilentlyContinue
                if ($hit) {
                    $appLogMentions += [PSCustomObject]@{
                        User = $pt.User; App = $src.App; File = $_.FullName
                        Line = ($hit.Line -replace '\s+', ' ')
                    }
                }
            }
        }
    }
}

Save-Csv '10_app_logs_collected.csv' $appLogStats
if ($appLogMentions) { Save-Csv '10b_app_log_telemost_mentions.csv' $appLogMentions }

Add-Summary "APPLICATION LOGS (copied to app_logs\<user>\<app>\)" ($appLogStats | ForEach-Object {
    "- [$($_.App)] $($_.User): files=$($_.Files) size=$($_.SizeKB)KB too-big-skipped=$($_.SkippedTooBig) src=$($_.Source) $($_.Note)"
})
$diskMentions = @($appLogMentions | Where-Object { $_.App -eq 'disk' })
if ($appLogMentions) {
    Add-Summary "APPLICATION LOGS (Telemost mentions in Disk logs)" ($appLogMentions | Select-Object -First 30 | ForEach-Object {
        "- [$($_.App)/$($_.User)] $($_.Line)"
    })
}
$appLogFileCount = 0
if ($appLogStats) { $appLogFileCount = ($appLogStats | Measure-Object -Property Files -Sum).Sum }
Write-Host ("  App log files copied: {0} | Telemost mentions inside them: {1} (in Disk logs: {2})" -f `
    $appLogFileCount, @($appLogMentions).Count, @($diskMentions).Count) -ForegroundColor Yellow

# ============================================================
# 10. Installation source hypothesis
# ============================================================
Write-Step "Building installation source hypothesis"

$hypothesis = New-Object System.Collections.ArrayList
[void]$hypothesis.Add("TELEMOST INSTALLATION SOURCE HYPOTHESIS (EXE focus):")

if ($regHits) {
    $exeType = @($regHits | Where-Object { $_.UninstallString -match '\.exe' -or $_.QuietUninstallString -match '\.exe' }).Count
    if ($exeType -gt 0) {
        [void]$hypothesis.Add(" [EXE installer] - uninstall string references an .exe. Non-MSI installer: Telemost own updater, WinGet, SCCM package, or a script. Check InstallSource/InstallLocation in 01_registry_uninstall.csv.")
    } else {
        [void]$hypothesis.Add(" [Installer type unclear] - uninstall string does not reference an .exe in registry. Review UninstallString in 01_registry_uninstall.csv manually.")
    }
}
if ($events) {
    [void]$hypothesis.Add(" [Windows events] - Telemost mentions found in event logs (incl. Security/Sysmon/TaskScheduler/PowerShell). Open 02_events.csv and the 02_application.evtx dump for the triggering process/user.")
}
if ($execTraces -and @($execTraces).Count -gt 0) {
    [void]$hypothesis.Add(" [Execution traces] - non-event sources found run/install times for Telemost binaries in 02b_execution_traces.csv. These work even when a manual EXE install leaves NO Application event: compare BAM/UserAssist last-run time with InstallDate.")
}
if ($sccmFindings) {
    [void]$hypothesis.Add(" [SCCM / ConfigMgr] - !!! mentions found in C:\Windows\CCM\Logs. Strong sign of SCCM deployment. Check AppEnforce.log (who triggered) and AppIntentEval.log (deployment/app-id).")
}
if ($gpFindings) {
    [void]$hypothesis.Add(" [Group Policy] - mentions in GP/setupapi/WU logs. Possibly GPO Software Installation or a startup/logon script.")
}
if ($schedTasks) {
    [void]$hypothesis.Add(" [Scheduled Task] - Telemost tasks found. Likely the auto-updater (self-updates the version). Check State, Actions and LastRun in 06_scheduled_tasks.csv.")
}
if ($appx) {
    [void]$hypothesis.Add(" [Appx/Store] - installed as an Appx package. Source: Microsoft Store or sideload.")
}
if ($winGet) {
    [void]$hypothesis.Add(" [WinGet] - package visible in 'winget list'. Installation may have been via winget (could pin an old version).")
}
if ($wuHistory) {
    [void]$hypothesis.Add(" [Windows Update / WSUS] - Telemost appears in WU history (possibly a WSUS-deployed package).")
}
if ($diskMentions) {
    [void]$hypothesis.Add(" [Yandex Disk] - !!! Disk's own logs mention Telemost. Disk is a known carrier for Telemost installs/updates. Read the lines in 10b_app_log_telemost_mentions.csv and the full files in app_logs\<user>\disk\.")
}
if ($appLogStats | Where-Object { $_.App -eq 'telemost' -and $_.Files -gt 0 }) {
    [void]$hypothesis.Add(" [Telemost own logs] - the client's own logs were collected in app_logs\<user>\telemost\. They carry the update/relaunch timeline (self-update vs fresh install) - compare with InstallDate from the registry.")
}
if ($prefetch) {
    [void]$hypothesis.Add(" [Prefetch] - installer/updater exe ran (see dates in 08_prefetch.csv). Compare run date with InstallDate from registry.")
}

# Take this reading BEFORE appending the caveats below, otherwise they would count
# as "a source was identified" and suppress the fallback advice.
$sourceIdentified = ($hypothesis.Count -gt 1)

if ($runMode -eq 'User') {
    [void]$hypothesis.Add(" [!] USER MODE - the admin-only sources listed under SOURCES NOT CHECKED were not queried at all. Nothing above rules a source out; re-run elevated with -Mode Admin for a verdict.")
}
if ($null -eq $cmdLineAudit -or $cmdLineAudit -eq 0) {
    [void]$hypothesis.Add(" [!] Command-line auditing is off (ProcessCreationIncludeCmdLine_Enabled), so even present 4688 events cannot show HOW the installer was invoked - see 02c_audit_policy.txt.")
}

if (-not $sourceIdentified) {
    [void]$hypothesis.Add(" Source could not be determined - collected artifacts insufficient. Recommended next steps:")
    if ($runMode -eq 'User') {
        [void]$hypothesis.Add("   - FIRST: re-run elevated (-Mode Admin) - the decisive sources (BAM, Prefetch, Security 4688, SCCM logs, all user hives) were skipped in this run")
    }
    [void]$hypothesis.Add("   - capture a Process Monitor (procmon) trace of the next install")
    [void]$hypothesis.Add("   - check centrally in AD/SCCM console which deployments target this PC")
    [void]$hypothesis.Add("   - enable extended Group Policy logging (GPO Policy Logging)")
}

Add-Summary "HYPOTHESIS" $hypothesis
Write-Host ""
Write-Host "=== Hypothesis ===" -ForegroundColor Green
$hypothesis | ForEach-Object { Write-Host $_ }

# ---------- what was NOT looked at ----------
Add-Summary "SOURCES NOT CHECKED (mode: $runMode)" $(if (@($skippedSources).Count -gt 0) {
    @("These sources were not queried in this run. 'Not found' above says nothing about them:") + $skippedSources
} else { "None - all sources were queried." })
if (@($skippedSources).Count -gt 0) {
    Write-Host ""
    Write-Host "=== Sources NOT checked in this run ($runMode mode) ===" -ForegroundColor Yellow
    $skippedSources | ForEach-Object { Write-Host "  - $_" -ForegroundColor Yellow }
}

# ---------- unload hives we mounted ----------
# Must happen before the report is written: reg unload fails while PowerShell still
# holds a handle from Get-ChildItem, so drop references and collect first.
if (@($mountedHives).Count -gt 0) {
    # Every RegistryKey object still referenced keeps the hive open, so release them all.
    $userHives = $null; $guidKeys = $null; $gk = $null; $uk = $null; $bamUserKeys = $null
    [System.GC]::Collect()
    [System.GC]::WaitForPendingFinalizers()
    $unloadFails = @()
    foreach ($mk in $mountedHives) {
        $null = & reg.exe unload "HKU\$mk" 2>&1
        if ($LASTEXITCODE -ne 0) { $unloadFails += $mk }
    }
    if ($unloadFails) {
        Add-Summary "HIVE CLEANUP" "Failed to unload: $($unloadFails -join ', '). Unload manually: reg unload HKU\<key>"
        Write-Host ("  WARNING: could not unload hive(s): {0}" -f ($unloadFails -join ', ')) -ForegroundColor Red
    } else {
        Add-Summary "HIVE CLEANUP" "All temporarily mounted user hives unloaded."
    }
}

# ---------- final report ----------
$reportPath = Save-Text 'summary_report.txt' ($summary -join "`n")
Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Green
Write-Host "Report:       $reportPath"
Write-Host "Artifacts:    $OutDir"
$logCount = @(Get-ChildItem -Path $LogDir -File -ErrorAction SilentlyContinue).Count
$appCount = @(Get-ChildItem -Path $AppLogRoot -Recurse -File -ErrorAction SilentlyContinue).Count
Write-Host "Software logs: $LogDir ($logCount file(s))"
Write-Host "App logs:      $AppLogRoot ($appCount file(s), per user/app subfolders)"
Write-Host ""
if ($runMode -eq 'User') {
    Write-Host "Mode was USER - for a conclusive answer collect again elevated:" -ForegroundColor Yellow
    Write-Host "  powershell -ExecutionPolicy Bypass -File `"$PSCommandPath`" -Mode Admin" -ForegroundColor Yellow
    Write-Host ""
}