<#
.SYNOPSIS
    Удаляет пер-пользовательские (per-user) установки Яндекс.Телемоста для
    ТЕКУЩЕГО пользователя, сохраняя машинную (per-machine) установку в
    Program Files, и чинит ассоциацию протокола telemost://, чтобы она
    указывала на машинный клиент и бралась из HKLM.

.DESCRIPTION
    Рассчитан на запуск В КОНТЕКСТЕ ПОЛЬЗОВАТЕЛЯ (например, GPO User
    Configuration -> Logon Script), БЕЗ повышения прав до администратора.
    Повышение не требуется, потому что:
      - per-user MSI зарегистрирован в HKCU текущего пользователя, и его
        удаление (msiexec /x) выполняется в контексте этого же пользователя;
      - старый exe-Телемост тоже стоит в профиле пользователя и удаляется
        его же правами;
      - пользовательская ассоциация telemost:// живёт в
        HKCU\Software\Classes и правится без прав администратора.

    ЛОГИКА:
      1. Проверяем, что машинный (per-machine) Телемост установлен в HKLM
         (Yandex.Telemost.2.Installer :: InstallDir указывает на Program Files).
         Если машинной установки нет — НИЧЕГО не удаляем: иначе пользователь
         останется вообще без клиента. Просто сообщаем и выходим.
      2. Ищем ДРУГИЕ (per-user) установки для текущего пользователя:
           - per-user MSI:  HKCU\Software\Yandex\Yandex.Telemost.2.Installer :: ProductCode
           - старый exe:    HKCU\Software\Yandex\Yandex.Telemost.Installer   :: InstallerPath
      3. Если найдены — удаляем их (перед удалением гасим процессы Телемоста),
         оставляя машинный клиент в Program Files нетронутым. Заодно убираем
         пользовательскую ассоциацию telemost:// (HKCU\Software\Classes\telemost),
         которая могла указывать на уже удалённый per-user клиент.
      4. Проверяем реестр: telemost:// теперь резолвится из HKLM и указывает
         на существующий exe в Program Files (в HKCU перекрывающей записи
         больше нет).

.NOTES
    PowerShell 5.1+. Права администратора НЕ нужны.
#>

#Requires -Version 5.1

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

# --- Конфигурация -----------------------------------------------------------

# Машинная (per-machine) установка нового MSI-клиента. Проверяем обе ветки
# на случай перенаправления битности (WOW6432Node).
$MachineInstallerKeys = @(
    'HKLM:\SOFTWARE\Yandex\Yandex.Telemost.2.Installer',
    'HKLM:\SOFTWARE\WOW6432Node\Yandex\Yandex.Telemost.2.Installer'
)
$InstallDirValueName  = 'InstallDir'
$ProductCodeValueName = 'ProductCode'
$ClientExeName        = 'YandexTelemost.exe'

# Пер-пользовательский MSI-клиент (HKCU).
$PeruserMsiKey        = 'HKCU:\Software\Yandex\Yandex.Telemost.2.Installer'

# Старый exe-клиент (HKCU).
$LegacyExeKey         = 'HKCU:\Software\Yandex\Yandex.Telemost.Installer'
$LegacyExeValueName   = 'InstallerPath'
$LegacyUninstallArgs  = @('-uninstallcomplete', '-silent')

# Ассоциация протокола. Относительный путь одинаков в HKLM и HKCU.
$ProtocolRelKey       = 'Software\Classes\telemost'
$ProtocolCommandRel   = "$ProtocolRelKey\shell\open\command"
$HklmProtocolCommand  = "HKLM:\$ProtocolCommandRel"
$HkcuProtocolRoot     = "HKCU:\$ProtocolRelKey"
$HkcuProtocolCommand  = "HKCU:\$ProtocolCommandRel"

# Процессы, которые держат файлы Телемоста и мешают удалению.
$ProcessNames         = @('Telemost', 'Yandex.Telemost', 'YandexTelemost', 'TelemostInstaller')
$ProcessWaitSec       = 5

# --- Логирование ------------------------------------------------------------
# Скрипт работает в контексте пользователя, поэтому пишем в его LOCALAPPDATA.
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

# Читает одно значение реестра, возвращая $null при отсутствии ключа/значения.
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

# Проверяет, что строка — валидный GUID {XXXXXXXX-...}. ProductCode для MSI
# всегда в фигурных скобках; заодно защищает от инъекции аргументов в msiexec.
function Test-ProductCode {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) { return $false }
    return $Value -match '^\{[0-9A-Fa-f]{8}-([0-9A-Fa-f]{4}-){3}[0-9A-Fa-f]{12}\}$'
}

# Достаёт путь к .exe из строки команды shell\open\command.
function Get-ExeFromCommand {
    param([string]$Command)
    if ([string]::IsNullOrWhiteSpace($Command)) { return $null }
    if ($Command -match '^\s*"([^"]+)"') { return [Environment]::ExpandEnvironmentVariables($Matches[1]) }
    if ($Command -match '^\s*(\S+)')     { return [Environment]::ExpandEnvironmentVariables($Matches[1]) }
    return $null
}

# Гасит процессы Телемоста текущего пользователя, чтобы освободить файлы
# перед удалением. Работает в пользовательском контексте — трогает только
# процессы этого пользователя.
function Stop-TelemostProcesses {
    $running = Get-Process -ErrorAction SilentlyContinue |
        Where-Object { $ProcessNames -contains $_.ProcessName }
    if (-not $running) {
        Write-Log "Запущенных процессов Телемоста не найдено."
        return
    }
    foreach ($proc in $running) {
        try {
            Stop-Process -Id $proc.Id -Force -ErrorAction Stop
            Write-Log "Остановлен процесс $($proc.ProcessName) (PID $($proc.Id))."
        } catch {
            Write-Log "Не удалось остановить $($proc.ProcessName) (PID $($proc.Id)): $($_.Exception.Message)" 'WARN'
        }
    }
    Write-Log "Ждём $ProcessWaitSec с освобождения файловых блокировок..."
    Start-Sleep -Seconds $ProcessWaitSec
}

# ============================================================================
#  Основной поток
# ============================================================================
try {
    if (-not (Test-Path -LiteralPath $LogDir)) {
        New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
    }
    Write-Log "=== Очистка per-user Телемоста для '$env:USERNAME' на '$env:COMPUTERNAME' ==="

    # --- Шаг 1: машинная установка в HKLM (её оставляем) --------------------
    Write-Log "[Шаг 1] Проверяем машинную (per-machine) установку в HKLM."
    $machineInstallDir  = $null
    $machineProductCode = $null
    foreach ($key in $MachineInstallerKeys) {
        $dir = Get-RegistryValueOrNull -Path $key -Name $InstallDirValueName
        if ($dir) {
            $machineInstallDir  = $dir
            $machineProductCode = Get-RegistryValueOrNull -Path $key -Name $ProductCodeValueName
            Write-Log "  Машинный InstallDir найден в '$key': $machineInstallDir"
            break
        }
        Write-Log "  Нет InstallDir в '$key'."
    }

    if (-not $machineInstallDir) {
        Write-Log "Машинная установка Телемоста в HKLM не найдена. Чтобы не оставить" 'WARN'
        Write-Log "пользователя без клиента, per-user установки НЕ удаляем. Выход." 'WARN'
        Write-Log "=== Завершено (без изменений) ==="
        exit 0
    }

    $machineExe = Join-Path $machineInstallDir $ClientExeName
    if (Test-Path -LiteralPath $machineExe) {
        Write-Log "  Машинный клиент на диске присутствует: $machineExe" 'OK'
    } else {
        Write-Log "  ВНИМАНИЕ: машинный exe по пути '$machineExe' не найден. Реестр есть," 'WARN'
        Write-Log "  но файла нет — удаление per-user клиентов может оставить систему без" 'WARN'
        Write-Log "  рабочего клиента. Продолжаем, но зафиксируйте это в проверке (Шаг 4)." 'WARN'
    }

    # --- Шаг 2: ищем per-user установки текущего пользователя ---------------
    Write-Log "[Шаг 2] Ищем другие (per-user) установки Телемоста для пользователя."

    # 2a. per-user MSI (HKCU).
    $peruserProductCode = Get-RegistryValueOrNull -Path $PeruserMsiKey -Name $ProductCodeValueName
    $peruserInstallDir  = Get-RegistryValueOrNull -Path $PeruserMsiKey -Name $InstallDirValueName
    $hasPeruserMsi      = $false
    if ($peruserProductCode) {
        if ($machineProductCode -and ($peruserProductCode -ieq $machineProductCode)) {
            # HKCU-запись указывает на тот же продукт, что и HKLM, — это машинная
            # регистрация, а не отдельный per-user клиент. Не трогаем.
            Write-Log "  HKCU ProductCode совпадает с машинным ($peruserProductCode) — это"
            Write-Log "  машинная регистрация, per-user MSI как таковой отсутствует."
        } elseif (-not (Test-ProductCode -Value $peruserProductCode)) {
            Write-Log "  HKCU ProductCode '$peruserProductCode' не похож на GUID — пропуск." 'WARN'
        } else {
            $hasPeruserMsi = $true
            Write-Log "  Найден per-user MSI: ProductCode=$peruserProductCode InstallDir='$peruserInstallDir'."
        }
    } else {
        Write-Log "  Per-user MSI (HKCU\...\Yandex.Telemost.2.Installer :: ProductCode) не найден."
    }

    # 2b. старый exe (HKCU).
    $legacyExePath = Get-RegistryValueOrNull -Path $LegacyExeKey -Name $LegacyExeValueName
    $hasLegacyExe  = $false
    if ($legacyExePath) {
        if ($legacyExePath -notmatch '\.exe$') {
            Write-Log "  Legacy InstallerPath не указывает на .exe ('$legacyExePath') — пропуск." 'WARN'
        } elseif (-not (Test-Path -LiteralPath $legacyExePath)) {
            Write-Log "  Legacy exe в реестре есть, но файла на диске нет ('$legacyExePath')." 'WARN'
            Write-Log "  Удаление запускать нечем; почистим только запись ассоциации на Шаге 3."
        } else {
            $hasLegacyExe = $true
            Write-Log "  Найден старый exe-клиент: $legacyExePath"
        }
    } else {
        Write-Log "  Старый exe-клиент (HKCU\...\Yandex.Telemost.Installer :: InstallerPath) не найден."
    }

    # --- Шаг 3: удаляем найденные per-user клиенты --------------------------
    if (-not $hasPeruserMsi -and -not $hasLegacyExe) {
        Write-Log "[Шаг 3] Per-user клиентов для удаления не найдено."
    } else {
        Write-Log "[Шаг 3] Удаляем per-user клиенты (машинный в Program Files сохраняем)."
        Stop-TelemostProcesses

        if ($hasPeruserMsi) {
            Write-Log "  Удаляем per-user MSI: msiexec /x `"$peruserProductCode`" /qn /norestart"
            try {
                $p = Start-Process msiexec.exe `
                        -ArgumentList '/x', $peruserProductCode, '/qn', '/norestart' `
                        -Wait -PassThru
                switch ($p.ExitCode) {
                    0       { Write-Log "  per-user MSI удалён успешно (0)." 'OK' }
                    1605    { Write-Log "  продукт уже не установлен (1605) — пропуск." }
                    3010    { Write-Log "  удалён, требуется перезагрузка (3010)." 'OK' }
                    default { Write-Log "  msiexec вернул код $($p.ExitCode)." 'WARN' }
                }
            } catch {
                Write-Log "  Ошибка запуска msiexec: $($_.Exception.Message)" 'ERROR'
            }
        }

        if ($hasLegacyExe) {
            Write-Log "  Удаляем старый exe: `"$legacyExePath`" $($LegacyUninstallArgs -join ' ')"
            try {
                $p = Start-Process -FilePath $legacyExePath -ArgumentList $LegacyUninstallArgs -Wait -PassThru
                if ($p.ExitCode -eq 0) {
                    Write-Log "  старый exe удалён успешно (0)." 'OK'
                } else {
                    Write-Log "  деинсталлятор вернул код $($p.ExitCode)." 'WARN'
                }
            } catch {
                Write-Log "  Ошибка запуска деинсталлятора: $($_.Exception.Message)" 'ERROR'
            }
        }
    }

    # Убираем пользовательскую ассоциацию telemost:// НЕЗАВИСИМО от того, нашлись
    # ли клиенты: запись в HKCU\Software\Classes\telemost может остаться
    # осиротевшей от давно удалённого per-user клиента и всё равно перекрывать
    # машинную запись из HKLM. Раз машинная установка (Шаг 1) точно есть,
    # пользователю после чистки будет на что упасть через HKLM.
    Write-Log "[Шаг 3b] Чистим пользовательскую ассоциацию telemost:// (HKCU), если она есть."
    if (Test-Path -LiteralPath $HkcuProtocolRoot) {
        try {
            Remove-Item -LiteralPath $HkcuProtocolRoot -Recurse -Force -ErrorAction Stop
            Write-Log "  Удалена пользовательская ассоциация '$HkcuProtocolRoot'." 'OK'
        } catch {
            Write-Log "  Не удалось удалить '$HkcuProtocolRoot': $($_.Exception.Message)" 'ERROR'
        }
    } else {
        Write-Log "  Пользовательской ассоциации '$HkcuProtocolRoot' нет — чистить нечего."
    }

    # --- Шаг 4: проверяем ассоциацию telemost:// ----------------------------
    Write-Log "[Шаг 4] Проверяем, что telemost:// берётся из HKLM и указывает на"
    Write-Log "         существующий клиент в Program Files."
    $ok = $true

    # 4a. В HKCU не должно остаться перекрывающей записи.
    if (Test-Path -LiteralPath $HkcuProtocolCommand) {
        $hkcuCmd = Get-RegistryValueOrNull -Path $HkcuProtocolCommand -Name '(default)'
        Write-Log "  HKCU всё ещё содержит ассоциацию telemost:// -> $hkcuCmd" 'WARN'
        Write-Log "  Пока она есть, для пользователя приоритет у неё, а не у HKLM." 'WARN'
        $ok = $false
    } else {
        Write-Log "  В HKCU перекрывающей записи telemost:// нет — приоритет у HKLM." 'OK'
    }

    # 4b. В HKLM запись должна существовать.
    if (-not (Test-Path -LiteralPath $HklmProtocolCommand)) {
        Write-Log "  В HKLM нет '$ProtocolCommandRel' — машинная ассоциация отсутствует." 'ERROR'
        $ok = $false
    } else {
        $hklmCmd = Get-RegistryValueOrNull -Path $HklmProtocolCommand -Name '(default)'
        Write-Log "  HKLM ассоциация telemost:// -> $hklmCmd"
        $hklmExe = Get-ExeFromCommand -Command $hklmCmd

        if (-not $hklmExe) {
            Write-Log "  Не удалось извлечь путь к .exe из команды HKLM." 'ERROR'
            $ok = $false
        } elseif (-not (Test-Path -LiteralPath $hklmExe)) {
            Write-Log "  Клиент из HKLM не найден на диске: $hklmExe" 'ERROR'
            $ok = $false
        } else {
            Write-Log "  Клиент из HKLM существует: $hklmExe" 'OK'
            # 4c. И это должен быть машинный клиент из Program Files (совпадает с InstallDir из HKLM).
            $expectedExe = Join-Path $machineInstallDir $ClientExeName
            if ($hklmExe -ieq $expectedExe) {
                Write-Log "  Путь совпадает с машинным InstallDir из HKLM — ассоциация корректна." 'OK'
            } else {
                Write-Log "  ВНИМАНИЕ: HKLM-команда ('$hklmExe') не совпадает с ожидаемым" 'WARN'
                Write-Log "  машинным путём ('$expectedExe'). Проверьте вручную." 'WARN'
            }
        }
    }

    if ($ok) {
        Write-Log "=== Готово: per-user клиенты убраны, telemost:// резолвится из HKLM в Program Files ==="
        exit 0
    } else {
        Write-Log "=== Завершено с замечаниями по ассоциации (см. WARN/ERROR выше) ===" 'WARN'
        exit 1
    }
} catch {
    Write-Log "Необработанная ошибка: $($_.Exception.Message)" 'ERROR'
    if ($_.InvocationInfo) { Write-Log $_.InvocationInfo.PositionMessage 'ERROR' }
    exit 1
}
