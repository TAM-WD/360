<#
.SYNOPSIS
    Тихо удаляет ПЕР-ПОЛЬЗОВАТЕЛЬСКУЮ (per-user) MSI-установку Яндекс.Телемоста
    на всех профилях централизованно управляемого ПК.

.DESCRIPTION
    Рассчитан на раскатку через GPO (Computer Configuration -> Startup Script)
    от имени NT AUTHORITY\SYSTEM.

    ПОЧЕМУ ТАК, А НЕ ПРОСТО "msiexec /x" ПОД SYSTEM:
        ПО ставится в профиль пользователя (per-user MSI). Его регистрация
        Windows Installer лежит в HKCU вызывающего аккаунта. Если запустить
        msiexec /x {ProductCode} под SYSTEM, Installer вернёт 1605
        ("this action is only valid for products that are currently installed"),
        потому что для SYSTEM продукт не зарегистрирован. Поэтому ProductCode
        мы ЧИТАЕМ машинно (обходя хайвы всех пользователей), а САМО удаление
        выполняем В КОНТЕКСТЕ конкретного пользователя через назначенное задание.

    ЛОГИКА:
      1. Перебираем все реальные профили пользователей (ProfileList, SID S-1-5-21-*).
      2. Для каждого читаем HKCU-эквивалент:
           Software\Yandex\Yandex.Telemost.2.Installer :: ProductCode
         - залогиненные -> напрямую в HKEY_USERS\<SID>;
         - остальные    -> временной загрузкой NTUSER.DAT (reg load / reg unload).
      3. Если ProductCode найден и это валидный GUID — планируем удаление
         в контексте этого пользователя:
           msiexec /x "{ProductCode}" /qn /norestart
         Перед самим удалением (уже в пользовательском контексте) проверяем,
         что процесс Телемоста не запущен, и при необходимости завершаем его.
           - пользователь залогинен -> задание запускается СРАЗУ, ждём результат;
           - пользователь offline   -> задание "при входе" (AtLogOn) сработает
                                        при следующем логоне и самоистечёт.
      4. Если у пользователя записи больше нет, но осталось отложенное задание
         с прошлого раза — убираем его (janitor).

.NOTES
    Запускать под SYSTEM. Требует PowerShell 5.1+ и модуль ScheduledTasks.
#>

#Requires -Version 5.1

$ErrorActionPreference = 'Stop'

# --- Конфигурация -----------------------------------------------------------
$InstallerSubKey  = 'Software\Yandex\Yandex.Telemost.2.Installer'
$ProductValueName = 'ProductCode'
$ProfileList      = 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\ProfileList'

# Процессы, которые держат файлы Телемоста и мешают удалению.
$ProcessNames     = @('Telemost', 'Yandex.Telemost', 'YandexTelemost', 'TelemostInstaller')

# Имя-префикс назначенных заданий (одно задание на SID).
$TaskPrefix       = 'RemoveTelemostMSI'
# Через сколько дней отложенное (AtLogOn) задание самоистекает и удаляется.
$DeferDays        = 14
# Сколько секунд ждём завершения немедленного задания.
$ImmediateWaitSec = 300

# --- Логирование ------------------------------------------------------------
$LogPath = Join-Path $env:ProgramData 'telemost_msi_cleanup.log'

function Write-Log {
    param([string]$Message, [string]$Level = 'INFO')
    $line = "{0} [{1}] {2}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Level, $Message
    try { Add-Content -Path $LogPath -Value $line -Encoding UTF8 -ErrorAction Stop } catch { }
    Write-Host $line
}

# Проверяет, что строка — валидный GUID вида {XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}.
# ProductCode для MSI всегда в фигурных скобках; заодно это защищает от инъекции
# произвольных аргументов в командную строку msiexec.
function Test-ProductCode {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) { return $false }
    return $Value -match '^\{[0-9A-Fa-f]{8}-([0-9A-Fa-f]{4}-){3}[0-9A-Fa-f]{12}\}$'
}

# Читает ProductCode из корня хайва пользователя (HKU:\<SID> или загруженного узла).
function Get-ProductCode {
    param([string]$UserRoot)

    $key = "$UserRoot\$InstallerSubKey"
    if (-not (Test-Path $key)) { return $null }

    try {
        $value = (Get-ItemProperty -Path $key -Name $ProductValueName -ErrorAction Stop).$ProductValueName
    }
    catch { return $null }

    if ([string]::IsNullOrWhiteSpace($value)) { return $null }
    return $value.Trim()
}

# Собирает msiexec-действие для назначенного задания. Полезная нагрузка выполняется
# в контексте пользователя: сначала гасит процессы Телемоста, затем удаляет продукт.
# Кодируем в -EncodedCommand, чтобы не мучиться с экранированием кавычек/скобок.
function New-UninstallAction {
    param([string]$ProductCode)

    $namesLiteral = ($ProcessNames | ForEach-Object { "'$_'" }) -join ','
    $payload = @"
`$names = @($namesLiteral)
Get-Process -ErrorAction SilentlyContinue |
    Where-Object { `$names -contains `$_.ProcessName } |
    Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 3
`$p = Start-Process msiexec.exe -ArgumentList '/x','$ProductCode','/qn','/norestart' -Wait -PassThru
exit `$p.ExitCode
"@
    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($payload))
    return New-ScheduledTaskAction -Execute 'powershell.exe' `
        -Argument "-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -EncodedCommand $encoded"
}

# Удаляет отложенное задание для SID, если оно есть (idempotent-очистка).
function Remove-DeferredTask {
    param([string]$Sid)
    $name = "${TaskPrefix}_$Sid"
    $task = Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
    if ($task) {
        Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
        Write-Log "  [$Sid] удалено отложенное задание '$name'."
    }
}

# Немедленное удаление в контексте залогиненного пользователя: регистрируем задание,
# запускаем, ждём завершения, логируем код возврата msiexec, снимаем задание.
function Invoke-ImmediateUninstall {
    param([string]$Sid, [string]$ProductCode)

    $name = "${TaskPrefix}_$Sid"
    Remove-DeferredTask -Sid $Sid   # на случай, если задание "при входе" осталось

    $action    = New-UninstallAction -ProductCode $ProductCode
    $principal = New-ScheduledTaskPrincipal -UserId $Sid -LogonType Interactive -RunLevel Limited
    $settings  = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
                    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

    try {
        Register-ScheduledTask -TaskName $name -Action $action -Principal $principal `
            -Settings $settings -Force -ErrorAction Stop | Out-Null
    }
    catch {
        Write-Log "  [$Sid] не удалось зарегистрировать задание: $_" 'ERROR'
        return
    }

    Write-Log "  [$Sid] запуск удаления ProductCode=$ProductCode в контексте пользователя."
    try {
        Start-ScheduledTask -TaskName $name -ErrorAction Stop
    }
    catch {
        Write-Log "  [$Sid] не удалось запустить задание (нет активной сессии?): $_" 'WARN'
        Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
        return
    }

    # Ждём завершения.
    $waited = 0
    do {
        Start-Sleep -Seconds 5
        $waited += 5
        $state = (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue).State
    } while ($state -eq 'Running' -and $waited -lt $ImmediateWaitSec)

    $info = Get-ScheduledTaskInfo -TaskName $name -ErrorAction SilentlyContinue
    $code = if ($info) { $info.LastTaskResult } else { $null }

    switch ($code) {
        0       { Write-Log "  [$Sid] удаление завершено успешно (0)." 'OK' }
        1605    { Write-Log "  [$Sid] продукт уже не установлен (1605) — пропуск." }
        3010    { Write-Log "  [$Sid] удаление успешно, требуется перезагрузка (3010)." 'OK' }
        $null   { Write-Log "  [$Sid] не удалось получить код возврата задания." 'WARN' }
        default {
            if ($state -eq 'Running') {
                Write-Log "  [$Sid] задание всё ещё выполняется по истечении ${ImmediateWaitSec}s." 'WARN'
            } else {
                Write-Log "  [$Sid] задание вернуло код $code." 'WARN'
            }
        }
    }

    # Снимаем задание, только если оно уже не выполняется.
    if ($state -ne 'Running') {
        Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
    }
}

# Отложенное удаление для offline-пользователя: задание "при входе" (AtLogOn),
# которое самоистекает через $DeferDays дней. msiexec /x идемпотентен, поэтому
# повторный запуск на уже удалённом продукте безвреден (1605).
function Register-DeferredUninstall {
    param([string]$Sid, [string]$ProductCode)

    $name = "${TaskPrefix}_$Sid"
    $action    = New-UninstallAction -ProductCode $ProductCode
    $principal = New-ScheduledTaskPrincipal -UserId $Sid -LogonType Interactive -RunLevel Limited

    # AtLogOn для конкретного пользователя + граница окончания, чтобы задание
    # само удалилось после истечения (DeleteExpiredTaskAfter).
    $trigger  = New-ScheduledTaskTrigger -AtLogOn -User $Sid
    $end      = (Get-Date).AddDays($DeferDays).ToString('yyyy-MM-ddTHH:mm:ss')
    $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
                    -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
                    -DeleteExpiredTaskAfter (New-TimeSpan -Seconds 0)
    $trigger.EndBoundary = $end

    try {
        Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger `
            -Principal $principal -Settings $settings -Force -ErrorAction Stop | Out-Null
        Write-Log "  [$Sid] пользователь offline — удаление отложено (задание '$name' при следующем входе, срок до $end)."
    }
    catch {
        Write-Log "  [$Sid] не удалось зарегистрировать отложенное задание: $_" 'ERROR'
    }
}

# ============================================================================
#  Основной поток
# ============================================================================
Write-Log "=== Запуск очистки per-user MSI Телемоста на '$env:COMPUTERNAME' ==="

# Собираем реальные пользовательские профили (SID S-1-5-21-*), исключая системные.
$profiles = Get-ChildItem $ProfileList | Where-Object {
    $_.PSChildName -match '^S-1-5-21-'
} | ForEach-Object {
    [pscustomobject]@{
        Sid         = $_.PSChildName
        ProfilePath = (Get-ItemProperty $_.PSPath -Name ProfileImagePath -ErrorAction SilentlyContinue).ProfileImagePath
    }
} | Where-Object { $_.ProfilePath }

Write-Log ("Найдено пользовательских профилей: {0}" -f @($profiles).Count)

# SID-ы, чьи хайвы уже загружены (залогиненные пользователи).
$loadedSids = Get-ChildItem 'Registry::HKEY_USERS' | Select-Object -ExpandProperty PSChildName

foreach ($p in $profiles) {
    $sid       = $p.Sid
    $isLoaded  = $loadedSids -contains $sid
    $userRoot  = $null
    $mounted   = $false
    $mountName = "TLMSI_$sid"

    try {
        if ($isLoaded) {
            $userRoot = "Registry::HKEY_USERS\$sid"
            Write-Log "Пользователь SID=$sid (залогинен, '$($p.ProfilePath)')."
        }
        else {
            $ntuser = Join-Path $p.ProfilePath 'NTUSER.DAT'
            if (-not (Test-Path $ntuser)) {
                Write-Log "Пользователь SID=$sid: NTUSER.DAT не найден ('$ntuser') — пропуск." 'WARN'
                continue
            }
            Write-Log "Пользователь SID=$sid (offline). Монтируем '$ntuser'."
            $out = reg load "HKU\$mountName" "$ntuser" 2>&1
            if ($LASTEXITCODE -ne 0) {
                Write-Log "  reg load не удался для SID=$sid: $out" 'ERROR'
                continue
            }
            $mounted  = $true
            $userRoot = "Registry::HKEY_USERS\$mountName"
        }

        # --- Читаем ProductCode ---
        $productCode = Get-ProductCode -UserRoot $userRoot

        if (-not $productCode) {
            Write-Log "  [$sid] запись '$InstallerSubKey :: $ProductValueName' не найдена — нечего удалять."
            Remove-DeferredTask -Sid $sid   # чистим возможный "хвост" с прошлого раза
            continue
        }

        if (-not (Test-ProductCode -Value $productCode)) {
            Write-Log "  [$sid] ProductCode '$productCode' не похож на GUID — пропуск во избежание неверной команды." 'WARN'
            continue
        }

        Write-Log "  [$sid] найден ProductCode=$productCode. Команда: msiexec /x `"$productCode`" /qn /norestart"

        # --- Планируем/выполняем удаление в контексте пользователя ---
        if ($isLoaded) {
            Invoke-ImmediateUninstall  -Sid $sid -ProductCode $productCode
        }
        else {
            Register-DeferredUninstall -Sid $sid -ProductCode $productCode
        }
    }
    finally {
        if ($mounted) {
            # Обязательно выгружаем хайв, иначе NTUSER.DAT останется залоченным.
            [gc]::Collect()   # закрыть хендлы provider'а реестра
            Start-Sleep -Milliseconds 200
            $out = reg unload "HKU\$mountName" 2>&1
            if ($LASTEXITCODE -ne 0) {
                Write-Log "  reg unload не удался для SID=$sid: $out" 'ERROR'
            }
            else {
                Write-Log "  Хайв SID=$sid выгружен."
            }
        }
    }
}

Write-Log "=== Очистка завершена ==="
