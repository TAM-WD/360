<#
.SYNOPSIS
    Удаляет пользовательскую (HKCU) регистрацию протокола telemost, если она
    дублируется машинной (HKLM) регистрацией.

.DESCRIPTION
    Рассчитан на раскатку через GPO (Computer Configuration -> Startup Script)
    от имени NT AUTHORITY\SYSTEM.

    Логика:
      1. Проверяем наличие HKLM\Software\Classes\telemost\shell\open\command.
      2. Если ветка есть — перебираем ВСЕ профили пользователей на ПК.
         Для каждого пользователя проверяем в его ветке (HKCU-эквивалент)
         наличие Software\Classes\telemost\shell\open\command.
      3. Если запись у пользователя есть И есть машинная запись — удаляем
         пользовательскую ветку telemost.

    Под SYSTEM ветка HKCU принадлежит самому SYSTEM, поэтому доступ к веткам
    пользователей идёт через HKEY_USERS: для залогиненных — напрямую по SID,
    для остальных — временной загрузкой NTUSER.DAT (reg load / reg unload).

.NOTES
    Запускать под SYSTEM (нужны права на reg load и на удаление чужих веток).
#>

# --- Пути реестра -----------------------------------------------------------
$RelativeKey  = 'Software\Classes\telemost\shell\open\command'
$HklmKey      = "HKLM:\$RelativeKey"
$ProfileList  = 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\ProfileList'

# --- Логирование ------------------------------------------------------------
$LogPath = Join-Path $env:ProgramData 'telemost_cu_cleanup.log'

function Write-Log {
    param([string]$Message, [string]$Level = 'INFO')
    $line = "{0} [{1}] {2}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Level, $Message
    Add-Content -Path $LogPath -Value $line -Encoding UTF8
    Write-Host $line
}

Write-Log "=== Запуск очистки HKCU\telemost ==="

# --- Шаг 1: проверяем машинную запись --------------------------------------
if (-not (Test-Path $HklmKey)) {
    Write-Log "HKLM запись '$RelativeKey' не найдена — очистка не требуется. Выход."
    return
}
Write-Log "HKLM запись найдена. Проверяем пользователей."

# --- Вспомогательное: удалить пользовательскую ветку по корню HKU:\<SID> ----
function Remove-UserTelemost {
    param([string]$UserRoot, [string]$UserLabel)

    $userKey = "$UserRoot\$RelativeKey"
    if (-not (Test-Path $userKey)) {
        Write-Log "  [$UserLabel] запись telemost не найдена — пропуск."
        return
    }

    # Удаляем ветку telemost целиком (родитель нужной записи).
    $telemostRoot = "$UserRoot\Software\Classes\telemost"
    try {
        Remove-Item -Path $telemostRoot -Recurse -Force -ErrorAction Stop
        Write-Log "  [$UserLabel] удалена ветка '$telemostRoot'." 'OK'
    }
    catch {
        Write-Log "  [$UserLabel] ошибка удаления '$telemostRoot': $_" 'ERROR'
    }
}

# --- Шаг 2: собираем профили пользователей ---------------------------------
# Берём только реальные пользовательские SID (S-1-5-21-...), исключая системные.
$profiles = Get-ChildItem $ProfileList | Where-Object {
    $_.PSChildName -match '^S-1-5-21-'
} | ForEach-Object {
    [pscustomobject]@{
        Sid         = $_.PSChildName
        ProfilePath = (Get-ItemProperty $_.PSPath -Name ProfileImagePath -ErrorAction SilentlyContinue).ProfileImagePath
    }
} | Where-Object { $_.ProfilePath }

Write-Log ("Найдено пользовательских профилей: {0}" -f $profiles.Count)

# SID-ы, чьи ветки уже загружены в HKEY_USERS (залогиненные пользователи).
$loadedSids = (Get-ChildItem 'Registry::HKEY_USERS' |
    Select-Object -ExpandProperty PSChildName)

foreach ($p in $profiles) {
    $sid = $p.Sid

    if ($loadedSids -contains $sid) {
        # Пользователь залогинен — ветка уже смонтирована.
        Write-Log "Пользователь SID=$sid (загружен, '$($p.ProfilePath)')."
        Remove-UserTelemost -UserRoot "Registry::HKEY_USERS\$sid" -UserLabel $sid
    }
    else {
        # Профиль не загружен — подгружаем NTUSER.DAT во временный узел.
        $ntuser = Join-Path $p.ProfilePath 'NTUSER.DAT'
        if (-not (Test-Path $ntuser)) {
            Write-Log "Пользователь SID=$sid: NTUSER.DAT не найден ('$ntuser') — пропуск." 'WARN'
            continue
        }

        $mountName = "TL_$sid"
        Write-Log "Пользователь SID=$sid (не загружен). Монтируем '$ntuser'."

        # reg load — внешняя команда, работаем через неё.
        $load = reg load "HKU\$mountName" "$ntuser" 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Log "  reg load не удался для SID=$sid: $load" 'ERROR'
            continue
        }

        try {
            Remove-UserTelemost -UserRoot "Registry::HKEY_USERS\$mountName" -UserLabel $sid
        }
        finally {
            # Обязательно выгружаем, иначе NTUSER.DAT останется залоченным.
            [gc]::Collect()  # закрыть хендлы, оставленные PowerShell provider'ом
            Start-Sleep -Milliseconds 200
            $unload = reg unload "HKU\$mountName" 2>&1
            if ($LASTEXITCODE -ne 0) {
                Write-Log "  reg unload не удался для SID=$sid: $unload" 'ERROR'
            }
            else {
                Write-Log "  Хайв SID=$sid выгружен."
            }
        }
    }
}

Write-Log "=== Очистка завершена ==="
