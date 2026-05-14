# Скрипт предназначен для удаления старого ПО Телемоста и обновления обработчика протокола telemost:// для открытия ссылок Телемоста в .msi клиенте.
# Логика:
#   1. Проверяем наличие старого инсталлятора (HKCU\...\Yandex.Telemost.Installer\InstallerPath).
#   2. Получаем директорию новой версии Telemost (HKLM приоритетнее, иначе HKCU из Yandex.Telemost.2.Installer\InstallDir).
#   3. Если найден старый инсталлятор И есть директория новой версии - запускаем UninstallString от админа и ждём завершения.
#   4. Если найден новый путь установки, перезаписываем обработку протокола telemost://.

#Requires -Version 5.1

[CmdletBinding()]
param(
    # SID исходного (неэлевированного) пользователя — передаётся в дочерний процесс
    # после UAC, чтобы можно было читать его HKCU через HKEY_USERS\<SID>.
    [string]$OriginalUserSid
)

$ErrorActionPreference = 'Stop'

# --- Настройки запуска ---
# $AskForAdmin = $true  -> при отсутствии прав админа скрипт перезапустится с UAC.
# $AskForAdmin = $false -> повышение прав не запрашивается (часть действий может не выполниться).
# $DoNotExit   = $true  -> в конце работы и при ошибке окно ждёт нажатия Enter.
# $DoNotExit   = $false -> скрипт завершается сразу, без паузы.
$AskForAdmin = $false
$DoNotExit   = $false

function Pause-Exit {
    param([int]$Code = 0)
    if ($DoNotExit) {
        Write-Host ""
        Read-Host "Нажмите Enter для выхода"
    }
    exit $Code
}

function Test-IsAdministrator {
    $current = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($current)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

# --- Самоповышение прав до администратора ---
if ($AskForAdmin -and -not (Test-IsAdministrator)) {
    Write-Host "Требуются права администратора. Перезапуск скрипта с повышением..."
    $scriptPath = $MyInvocation.MyCommand.Path
    if (-not $scriptPath) {
        Write-Warning "Не удалось определить путь к скрипту для самоперезапуска."
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
        Write-Warning "Не удалось перезапустить с правами администратора: $($_.Exception.Message)"
        Pause-Exit 1
    }
} elseif (-not (Test-IsAdministrator)) {
    Write-Warning "Скрипт запущен без прав администратора (AskForAdmin=`$false). Часть действий может не выполниться."
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

# Возвращает массив путей реестра, по которым нужно искать значение для текущего HKCU-подключа.
# Если скрипт был элевирован (передан -OriginalUserSid), HKCU теперь принадлежит админу,
# поэтому дополнительно проверяем hive исходного пользователя через HKEY_USERS\<SID>.
function Get-HkcuSearchPaths {
    param([Parameter(Mandatory)] [string] $SubPath)  # например 'Software\Yandex\Yandex.Telemost.Installer'
    $paths = @("HKCU:\$SubPath")
    if ($OriginalUserSid) {
        $paths += "Registry::HKEY_USERS\$OriginalUserSid\$SubPath"
    }
    return $paths
}

# Подробное чтение значения реестра с логированием:
#   - существует ли ветка (ключ),
#   - доступна ли она для чтения (нет ли AccessDenied),
#   - существует ли искомое значение.
# Возвращает значение или $null. Печатает строки вида [BRANCH OK]/[BRANCH MISS]/[DENIED]/[VALUE OK]/[VALUE MISS]/[ERROR].
function Read-RegistryValueVerbose {
    param(
        [Parameter(Mandatory)] [string] $Path,
        [Parameter(Mandatory)] [string] $Name
    )
    # 1. Проверка существования ветки.
    $branchExists = $false
    try {
        $branchExists = Test-Path -LiteralPath $Path -ErrorAction Stop
    } catch [System.Security.SecurityException] {
        Write-Host "  [DENIED     ] Нет прав на доступ к ветке: $Path"
        return $null
    } catch {
        Write-Host "  [ERROR      ] Проверка ветки '$Path' упала: $($_.Exception.GetType().Name): $($_.Exception.Message)"
        return $null
    }
    if (-not $branchExists) {
        Write-Host "  [BRANCH MISS] Ветка не существует: $Path"
        return $null
    }
    Write-Host "  [BRANCH OK  ] Ветка найдена: $Path"

    # 2. Дополнительная проверка прав на чтение свойств ветки (на случай ACL deny на values).
    try {
        Get-ItemProperty -LiteralPath $Path -ErrorAction Stop | Out-Null
    } catch [System.Security.SecurityException] {
        Write-Host "  [DENIED     ] Нет прав на чтение значений ветки: $Path"
        return $null
    } catch [System.UnauthorizedAccessException] {
        Write-Host "  [DENIED     ] UnauthorizedAccess при чтении ветки: $Path"
        return $null
    } catch {
        # сюда могут попадать и нормальные ситуации (пустая ветка) — не считаем критичным.
    }

    # 3. Чтение конкретного значения.
    try {
        $item  = Get-ItemProperty -LiteralPath $Path -Name $Name -ErrorAction Stop
        $value = $item.$Name
        if ($null -eq $value -or ($value -is [string] -and $value -eq '')) {
            Write-Host "  [VALUE MISS ] Запись '$Name' пуста в $Path"
            return $null
        }
        Write-Host "  [VALUE OK   ] $Path :: $Name = $value"
        return $value
    } catch [System.Security.SecurityException] {
        Write-Host "  [DENIED     ] Нет прав на чтение '$Name' в $Path"
        return $null
    } catch [System.UnauthorizedAccessException] {
        Write-Host "  [DENIED     ] UnauthorizedAccess на '$Name' в $Path"
        return $null
    } catch [System.Management.Automation.PSArgumentException] {
        Write-Host "  [VALUE MISS ] Запись '$Name' не найдена в $Path"
        return $null
    } catch [System.Management.Automation.ItemNotFoundException] {
        Write-Host "  [VALUE MISS ] Запись '$Name' не найдена в $Path"
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
        Write-Host "Запущено с элевацией; HKCU исходного пользователя: HKEY_USERS\$OriginalUserSid"
    }

    # --- Шаг 1: проверяем старый инсталлятор и забираем UninstallString ---
    Write-Host "[Шаг 1] Поиск старого инсталлятора Yandex.Telemost.Installer..."
    $oldInstallerPath = Get-HkcuValue -SubPath 'Software\Yandex\Yandex.Telemost.Installer' -Name 'InstallerPath'

    $uninstallString = $null
    if ($oldInstallerPath) {
        Write-Host "Найден старый инсталлятор: $oldInstallerPath"
        Write-Host "[Шаг 1] Поиск UninstallString..."
        $uninstallString = Get-HkcuValue `
            -SubPath 'Software\Microsoft\Windows\CurrentVersion\Uninstall\YandexTelemost' `
            -Name 'UninstallString'
        if ($uninstallString) {
            $uninstallString = $uninstallString.TrimEnd() + ' -silent'
            Write-Host "UninstallString: $uninstallString"
        } else {
            Write-Host "UninstallString не найден в HKCU\...\Uninstall\YandexTelemost"
        }
    } else {
        Write-Host "Старый инсталлятор (Yandex.Telemost.Installer) не найден - пропускаем удаление."
    }

    # --- Шаг 2: ищем InstallDir новой версии (приоритет HKLM, потом HKCU) ---
    Write-Host "[Шаг 2] Поиск InstallDir новой версии Telemost..."
    Write-Host "[Шаг 2] PowerShell процесс: PID=$PID, 64-bit=$([Environment]::Is64BitProcess), OS 64-bit=$([Environment]::Is64BitOperatingSystem)"

    # Проверяем обе ветки HKLM: нативную и WOW6432Node — на случай разрядного редиректа.
    $hklmCandidates = @(
        'HKLM:\SOFTWARE\Yandex\Yandex.Telemost.2.Installer',
        'HKLM:\SOFTWARE\WOW6432Node\Yandex\Yandex.Telemost.2.Installer'
    )
    $installDirHKLM = $null
    foreach ($p in $hklmCandidates) {
        Write-Host "[Шаг 2] Проверяем HKLM: $p :: InstallDir"
        $v = Read-RegistryValueVerbose -Path $p -Name 'InstallDir'
        if ($v) { $installDirHKLM = $v; break }
    }

    Write-Host "[Шаг 2] Проверяем HKCU (включая hive исходного пользователя при элевации)..."
    $installDirHKCU = $null
    foreach ($p in (Get-HkcuSearchPaths -SubPath 'Software\Yandex\Yandex.Telemost.2.Installer')) {
        Write-Host "[Шаг 2] Проверяем HKCU/HKU: $p :: InstallDir"
        $v = Read-RegistryValueVerbose -Path $p -Name 'InstallDir'
        if ($v) { $installDirHKCU = $v; break }
    }

    if ($installDirHKLM) {
        $installDir = $installDirHKLM
        Write-Host "[Шаг 2] Выбран источник: HKLM"
    } elseif ($installDirHKCU) {
        $installDir = $installDirHKCU
        Write-Host "[Шаг 2] Выбран источник: HKCU"
    } else {
        $installDir = $null
    }

    if ($installDir) {
        Write-Host "InstallDir новой версии: $installDir"
    } else {
        Write-Host "InstallDir новой версии Telemost не найден ни в HKLM, ни в HKCU."
    }

    # --- Шаг 3: удаляем старую версию, если есть и старый инсталлятор, и новая версия ---
    if ($oldInstallerPath -and $installDir -and $uninstallString) {
        Write-Host "Запускаем удаление старой версии Telemost..."

        # Парсим UninstallString вида: "C:\...\TelemostInstaller.exe" -uninstallcomplete -silent
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
            Write-Warning "Исполняемый файл деинсталлятора не найден: $exePath"
        } else {
            $startArgs = @{
                FilePath = $exePath
                Wait     = $true
            }
            if ($exeArgs.Trim()) { $startArgs.ArgumentList = $exeArgs.Trim() }

            try {
                Start-Process @startArgs
                Write-Host "Деинсталляция завершена."
            } catch {
                Write-Warning "Ошибка при запуске деинсталлятора: $($_.Exception.Message)"
            }
        }
    } else {
        Write-Host "Условия для удаления не выполнены - деинсталляция пропущена."
    }

    # --- Шаг 4: обновляем обработчик протокола telemost:// ---
    if ($installDir) {
        $exeFullPath = Join-Path -Path $installDir -ChildPath 'YandexTelemost.exe'
        $newCommand  = '"{0}" --conf-url="%1"' -f $exeFullPath

        $protocolKeys = @(
            'Registry::HKEY_CLASSES_ROOT\telemost\shell\open\command',
            'Registry::HKEY_LOCAL_MACHINE\SOFTWARE\Classes\telemost\shell\open\command',
            'Registry::HKEY_CURRENT_USER\Software\Classes\telemost\shell\open\command'
        )
        foreach ($key in $protocolKeys) {
            if (Test-Path -LiteralPath $key) {
                Set-ItemProperty -LiteralPath $key -Name '(Default)' -Value $newCommand
                Write-Host "Обновлено $key -> $newCommand"
            } else {
                Write-Host "$key не существует - пропускаем."
            }
        }
    } else {
        Write-Host "Пропускаем обновление обработчика telemost:// - нет InstallDir."
    }

    Write-Host "Готово."
} catch {
    Write-Host ""
    Write-Host "ОШИБКА: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.InvocationInfo) {
        Write-Host $_.InvocationInfo.PositionMessage -ForegroundColor Red
    }
} finally {
    Pause-Exit
}
