# delete_disk_v3.ps1
# Назначение:
#   Тихое удаление Yandex Disk 3.x из всех локальных профилей пользователей
#   с последующей очисткой следов установки.
#
# Что удаляется:
#   - uninstall-записи Yandex Disk 3.x
#   - автозапуск
#   - startup approved entries
#   - scheduled tasks, связанные с YandexDisk2
#   - ярлыки
#   - каталоги %AppData%\Yandex\YandexDisk2 и %LocalAppData%\Yandex\YandexDisk2
#
# Что НЕ удаляется:
#   - пользовательские файлы синхронизации
#   - Yandex Disk 4.x
#
# Лог:
#   C:\Windows\Temp\Remove-YandexDisk3.log

$ErrorActionPreference = 'SilentlyContinue'

$LogFile    = 'C:\Windows\Temp\Remove-YandexDisk3.log'
$TimeoutSec = 300

function Write-Log {
    param([string]$Text)

    $dir = Split-Path -Path $LogFile -Parent
    if (-not (Test-Path $dir)) {
        New-Item -Path $dir -ItemType Directory -Force | Out-Null
    }

    $line = '{0}  {1}' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Text
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
}

function Get-ProfileList {
    Get-ChildItem 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\ProfileList' |
        Where-Object { $_.PSChildName -match '^S-1-5-21-' } |
        ForEach-Object {
            $p = Get-ItemProperty $_.PSPath
            [pscustomobject]@{
                SID         = $_.PSChildName
                ProfilePath = [Environment]::ExpandEnvironmentVariables($p.ProfileImagePath)
            }
        }
}

function Get-AccountNameFromSid {
    param(
        [string]$Sid,
        [string]$ProfilePath
    )

    try {
        (New-Object System.Security.Principal.SecurityIdentifier($Sid)).
            Translate([System.Security.Principal.NTAccount]).Value
    }
    catch {
        if ($ProfilePath) {
            Split-Path $ProfilePath -Leaf
        }
        else {
            $Sid
        }
    }
}

function Parse-CommandLine {
    param([string]$CommandLine)

    if ([string]::IsNullOrWhiteSpace($CommandLine)) {
        return $null
    }

    $m = [regex]::Match($CommandLine, '^\s*"(?<exe>[^"]+)"\s*(?<rest>.*)$')
    if ($m.Success) {
        return [pscustomobject]@{
            ExePath = $m.Groups['exe'].Value
            Args    = $m.Groups['rest'].Value.Trim()
        }
    }

    $parts = $CommandLine -split '\s+', 2
    [pscustomobject]@{
        ExePath = $parts[0]
        Args    = if ($parts.Count -gt 1) { $parts[1].Trim() } else { '' }
    }
}

function Get-YandexDisk3Entries {
    param(
        [string]$BaseKey
    )

    Get-ItemProperty "$BaseKey\Software\Microsoft\Windows\CurrentVersion\Uninstall\*" -ErrorAction SilentlyContinue |
        Where-Object {
            $_.DisplayName -match 'Yandex.*Disk' -and
            $_.DisplayVersion -match '^3(\.|$)'
        }
}

function Stop-YandexDisk3Processes {
    Write-Log 'Останавливаю процессы старого Yandex Disk 3.x'

    Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object {
            $_.Name -match '^YandexDisk3Installer' -or
            $_.ExecutablePath -match 'YandexDisk2' -or
            $_.CommandLine -match 'YandexDisk2'
        } |
        ForEach-Object {
            try {
                Invoke-CimMethod -InputObject $_ -MethodName Terminate | Out-Null
                Write-Log "Остановлен процесс: PID=$($_.ProcessId), Name=$($_.Name)"
            }
            catch {
                Write-Log "Не удалось остановить процесс: PID=$($_.ProcessId), Name=$($_.Name)"
            }
        }
}

function Invoke-SilentUninstall {
    param(
        [string]$CommandLine,
        [string]$UserName
    )

    $parsed = Parse-CommandLine -CommandLine $CommandLine
    if (-not $parsed) {
        Write-Log "[$UserName] Не удалось разобрать команду удаления"
        return $false
    }

    $exe = $parsed.ExePath
    $baseArgs = $parsed.Args

    if (-not (Test-Path $exe)) {
        Write-Log "[$UserName] Деинсталлятор не найден: $exe"
        return $false
    }

    if ($baseArgs -match '(?i)(^|\s)(-silent|/silent|/quiet|/S|/VERYSILENT)(\s|$)') {
        $fullArgs = $baseArgs
    }
    elseif ([string]::IsNullOrWhiteSpace($baseArgs)) {
        $fullArgs = '-silent'
    }
    else {
        $fullArgs = "$baseArgs -silent"
    }

    Write-Log "[$UserName] Запуск удаления: `"$exe`" $fullArgs"

    try {
        $p = Start-Process -FilePath $exe -ArgumentList $fullArgs -WindowStyle Hidden -PassThru
        $finished = $p.WaitForExit($TimeoutSec * 1000)

        if (-not $finished) {
            Write-Log "[$UserName] Таймаут $TimeoutSec сек. Завершаю процесс PID=$($p.Id)"
            Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
            return $false
        }

        Write-Log "[$UserName] Деинсталлятор завершился с кодом: $($p.ExitCode)"
        return $true
    }
    catch {
        Write-Log "[$UserName] Ошибка запуска деинсталлятора: $($_.Exception.Message)"
        return $false
    }
}

function Remove-RunValues {
    param(
        [string]$KeyPath,
        [string]$UserName
    )

    if (-not (Test-Path $KeyPath)) {
        return
    }

    $item = Get-ItemProperty -Path $KeyPath -ErrorAction SilentlyContinue
    if (-not $item) {
        return
    }

    $props = $item.PSObject.Properties | Where-Object {
        $_.Name -notin 'PSPath','PSParentPath','PSChildName','PSDrive','PSProvider'
    }

    foreach ($prop in $props) {
        $name  = [string]$prop.Name
        $value = [string]$prop.Value

        if (
            $name  -match 'Yandex.*Disk' -or
            $value -match 'Yandex.*Disk' -or
            $value -match 'YandexDisk2'
        ) {
            try {
                Remove-ItemProperty -Path $KeyPath -Name $name -Force
                Write-Log "[$UserName] Удалён автозапуск: $KeyPath -> $name"
            }
            catch {
                Write-Log "[$UserName] Не удалось удалить автозапуск: $KeyPath -> $name"
            }
        }
    }
}

function Remove-StartupApprovedValues {
    param(
        [string]$KeyPath,
        [string]$UserName
    )

    if (-not (Test-Path $KeyPath)) {
        return
    }

    $item = Get-ItemProperty -Path $KeyPath -ErrorAction SilentlyContinue
    if (-not $item) {
        return
    }

    $props = $item.PSObject.Properties | Where-Object {
        $_.Name -notin 'PSPath','PSParentPath','PSChildName','PSDrive','PSProvider'
    }

    foreach ($prop in $props) {
        $name = [string]$prop.Name
        if ($name -match 'Yandex.*Disk') {
            try {
                Remove-ItemProperty -Path $KeyPath -Name $name -Force
                Write-Log "[$UserName] Удалено StartupApproved-значение: $KeyPath -> $name"
            }
            catch {
                Write-Log "[$UserName] Не удалось удалить StartupApproved-значение: $KeyPath -> $name"
            }
        }
    }
}

function Remove-UninstallEntries {
    param(
        [string]$BaseKey,
        [string]$UserName
    )

    $root = "$BaseKey\Software\Microsoft\Windows\CurrentVersion\Uninstall"
    if (-not (Test-Path $root)) {
        return
    }

    Get-ChildItem -Path $root -ErrorAction SilentlyContinue | ForEach-Object {
        $p = Get-ItemProperty -Path $_.PSPath -ErrorAction SilentlyContinue
        if (
            $p.DisplayName -match 'Yandex.*Disk' -and
            $p.DisplayVersion -match '^3(\.|$)'
        ) {
            try {
                Remove-Item -Path $_.PSPath -Recurse -Force
                Write-Log "[$UserName] Удалён uninstall-ключ: $($p.DisplayName) $($p.DisplayVersion)"
            }
            catch {
                Write-Log "[$UserName] Не удалось удалить uninstall-ключ: $($_.PSPath)"
            }
        }
    }
}

function Remove-DirectorySafe {
    param(
        [string]$PathToRemove,
        [string]$UserName
    )

    if (Test-Path $PathToRemove) {
        try {
            Remove-Item -LiteralPath $PathToRemove -Recurse -Force
            Write-Log "[$UserName] Удалён каталог: $PathToRemove"
        }
        catch {
            Write-Log "[$UserName] Не удалось удалить каталог: $PathToRemove"
        }
    }
}

function Remove-Shortcuts {
    param(
        [string]$ProfilePath,
        [string]$UserName
    )

    $paths = @(
        (Join-Path $ProfilePath 'Desktop'),
        (Join-Path $ProfilePath 'AppData\Roaming\Microsoft\Windows\Start Menu\Programs'),
        (Join-Path $ProfilePath 'AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Startup')
    )

    foreach ($base in $paths) {
        if (-not (Test-Path $base)) {
            continue
        }

        Get-ChildItem -Path $base -File -Recurse -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -match 'Yandex.*Disk' } |
            ForEach-Object {
                try {
                    Remove-Item -LiteralPath $_.FullName -Force
                    Write-Log "[$UserName] Удалён ярлык/файл: $($_.FullName)"
                }
                catch {
                    Write-Log "[$UserName] Не удалось удалить ярлык/файл: $($_.FullName)"
                }
            }
    }
}

function Remove-TasksRelatedToProfile {
    param(
        [string]$ProfilePath,
        [string]$UserName
    )

    if (-not (Get-Command Get-ScheduledTask -ErrorAction SilentlyContinue)) {
        return
    }

    $needle = [regex]::Escape((Join-Path $ProfilePath 'AppData\Roaming\Yandex\YandexDisk2'))

    Get-ScheduledTask -ErrorAction SilentlyContinue | ForEach-Object {
        $task = $_
        $remove = $false

        foreach ($a in $task.Actions) {
            $exec = [string]$a.Execute
            $args = [string]$a.Arguments

            if (
                $exec -match 'Yandex.*Disk' -or
                $args -match 'Yandex.*Disk' -or
                $exec -match 'YandexDisk2' -or
                $args -match 'YandexDisk2' -or
                $exec -match $needle -or
                $args -match $needle
            ) {
                $remove = $true
                break
            }
        }

        if ($remove) {
            try {
                Unregister-ScheduledTask -TaskName $task.TaskName -TaskPath $task.TaskPath -Confirm:$false
                Write-Log "[$UserName] Удалено задание: $($task.TaskPath)$($task.TaskName)"
            }
            catch {
                Write-Log "[$UserName] Не удалось удалить задание: $($task.TaskPath)$($task.TaskName)"
            }
        }
    }
}

function Cleanup-YandexDisk3Profile {
    param(
        [string]$BaseKey,
        [string]$ProfilePath,
        [string]$UserName
    )

    Remove-RunValues -KeyPath "$BaseKey\Software\Microsoft\Windows\CurrentVersion\Run" -UserName $UserName
    Remove-RunValues -KeyPath "$BaseKey\Software\Microsoft\Windows\CurrentVersion\RunOnce" -UserName $UserName
    Remove-StartupApprovedValues -KeyPath "$BaseKey\Software\Microsoft\Windows\CurrentVersion\Explorer\StartupApproved\Run" -UserName $UserName

    Remove-TasksRelatedToProfile -ProfilePath $ProfilePath -UserName $UserName
    Remove-Shortcuts -ProfilePath $ProfilePath -UserName $UserName
    Remove-UninstallEntries -BaseKey $BaseKey -UserName $UserName

    Remove-DirectorySafe -PathToRemove (Join-Path $ProfilePath 'AppData\Roaming\Yandex\YandexDisk2') -UserName $UserName
    Remove-DirectorySafe -PathToRemove (Join-Path $ProfilePath 'AppData\Local\Yandex\YandexDisk2')   -UserName $UserName
}

function Process-Profile {
    param(
        [string]$BaseKey,
        [string]$ProfilePath,
        [string]$UserName
    )

    $entries = @(Get-YandexDisk3Entries -BaseKey $BaseKey)
    $roamingYD2 = Join-Path $ProfilePath 'AppData\Roaming\Yandex\YandexDisk2'
    $localYD2   = Join-Path $ProfilePath 'AppData\Local\Yandex\YandexDisk2'

    $hasFolders = (Test-Path $roamingYD2) -or (Test-Path $localYD2)

    if (($entries.Count -eq 0) -and (-not $hasFolders)) {
        return $false
    }

    Write-Log "[$UserName] Найдены следы Yandex Disk 3.x"

    foreach ($entry in $entries) {
        Write-Log "[$UserName] Обнаружена запись: $($entry.DisplayName) $($entry.DisplayVersion)"

        $commandLine = if (-not [string]::IsNullOrWhiteSpace($entry.QuietUninstallString)) {
            $entry.QuietUninstallString
        }
        else {
            $entry.UninstallString
        }

        if (-not [string]::IsNullOrWhiteSpace($commandLine)) {
            [void](Invoke-SilentUninstall -CommandLine $commandLine -UserName $UserName)
        }
        else {
            Write-Log "[$UserName] Команда удаления отсутствует, перехожу к cleanup"
        }
    }

    Cleanup-YandexDisk3Profile -BaseKey $BaseKey -ProfilePath $ProfilePath -UserName $UserName
    return $true
}

Write-Log '==== Старт удаления Yandex Disk 3.x ===='

Stop-YandexDisk3Processes

$foundAny = $false
$profiles = Get-ProfileList

foreach ($profile in $profiles) {
    $sid = $profile.SID
    $profilePath = $profile.ProfilePath

    if (-not (Test-Path $profilePath)) {
        continue
    }

    $userName = Get-AccountNameFromSid -Sid $sid -ProfilePath $profilePath

    $loadedHive    = Test-Path "Registry::HKEY_USERS\$sid"
    $tempHiveName  = "YD_$($sid -replace '[^A-Za-z0-9_]', '_')"
    $baseKey       = if ($loadedHive) { "Registry::HKEY_USERS\$sid" } else { "Registry::HKEY_USERS\$tempHiveName" }
    $loadedByScript = $false

    try {
        if (-not $loadedHive) {
            $ntUserDat = Join-Path $profilePath 'NTUSER.DAT'
            if (-not (Test-Path $ntUserDat)) {
                continue
            }

            & reg.exe load "HKU\$tempHiveName" "$ntUserDat" | Out-Null
            if ($LASTEXITCODE -ne 0) {
                Write-Log "[$userName] Не удалось загрузить hive: $ntUserDat"
                continue
            }

            $loadedByScript = $true
        }

        if (Process-Profile -BaseKey $baseKey -ProfilePath $profilePath -UserName $userName) {
            $foundAny = $true
        }
    }
    finally {
        if ($loadedByScript) {
            [gc]::Collect()
            [gc]::WaitForPendingFinalizers()
            & reg.exe unload "HKU\$tempHiveName" | Out-Null
        }
    }
}

# Дополнительно: машинные uninstall-записи 3.x, если вдруг есть
$machineRoots = @(
    'HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall',
    'HKLM:\Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall'
)

foreach ($root in $machineRoots) {
    if (-not (Test-Path $root)) {
        continue
    }

    Get-ChildItem -Path $root -ErrorAction SilentlyContinue | ForEach-Object {
        $p = Get-ItemProperty -Path $_.PSPath -ErrorAction SilentlyContinue
        if (
            $p.DisplayName -match 'Yandex.*Disk' -and
            $p.DisplayVersion -match '^3(\.|$)'
        ) {
            $foundAny = $true
            Write-Log "[Machine] Найдена запись: $($p.DisplayName) $($p.DisplayVersion)"

            $cmd = if (-not [string]::IsNullOrWhiteSpace($p.QuietUninstallString)) {
                $p.QuietUninstallString
            }
            else {
                $p.UninstallString
            }

            if (-not [string]::IsNullOrWhiteSpace($cmd)) {
                [void](Invoke-SilentUninstall -CommandLine $cmd -UserName 'Machine')
            }

            try {
                Remove-Item -Path $_.PSPath -Recurse -Force
                Write-Log "[Machine] Удалён uninstall-ключ: $($p.DisplayName) $($p.DisplayVersion)"
            }
            catch {
                Write-Log "[Machine] Не удалось удалить uninstall-ключ: $($_.PSPath)"
            }
        }
    }
}

if (-not $foundAny) {
    Write-Log 'Yandex Disk 3.x не найден. Ничего делать не нужно.'
}

Write-Log '==== Завершение удаления Yandex Disk 3.x ===='
exit 0
