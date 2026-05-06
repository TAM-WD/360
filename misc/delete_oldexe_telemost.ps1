# Скрипт предназначен для удаления старого ПО Телемоста и обновления обработчика протокола telemost:// для открытия ссылок Телемоста в .msi клиенте.
# Логика:
#   1. Проверяем наличие старого инсталлятора (HKCU\...\Yandex.Telemost.Installer\InstallerPath).
#   2. Получаем директорию новой версии Telemost (HKLM приоритетнее, иначе HKCU из Yandex.Telemost.2.Installer\InstallDir).
#   3. Если найден старый инсталлятор И есть директория новой версии - запускаем UninstallString от админа и ждём завершения.
#   4. Перезаписываем HKCR\telemost\shell\open\command значением "<InstallDir>\YandexTelemost.exe" --conf-url="%1".

#Requires -Version 5.1

[CmdletBinding()]
param()

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
        $childArgs = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', "`"$scriptPath`"")
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

try {
    # --- Шаг 1: проверяем старый инсталлятор и забираем UninstallString ---
    $oldInstallerPath = Get-RegistryValueOrNull `
        -Path 'HKCU:\Software\Yandex\Yandex.Telemost.Installer' `
        -Name 'InstallerPath'

    $uninstallString = $null
    if ($oldInstallerPath) {
        Write-Host "Найден старый инсталлятор: $oldInstallerPath"
        $uninstallString = Get-RegistryValueOrNull `
            -Path 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\YandexTelemost' `
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
    $installDirHKLM = Get-RegistryValueOrNull `
        -Path 'HKLM:\SOFTWARE\Yandex\Yandex.Telemost.2.Installer' `
        -Name 'InstallDir'
    $installDirHKCU = Get-RegistryValueOrNull `
        -Path 'HKCU:\Software\Yandex\Yandex.Telemost.2.Installer' `
        -Name 'InstallDir'

    if ($installDirHKLM) {
        $installDir = $installDirHKLM
    } elseif ($installDirHKCU) {
        $installDir = $installDirHKCU
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

        $hkcrKey = 'Registry::HKEY_CLASSES_ROOT\telemost\shell\open\command'
        if (-not (Test-Path -LiteralPath $hkcrKey)) {
            New-Item -Path $hkcrKey -Force | Out-Null
        }
        Set-ItemProperty -LiteralPath $hkcrKey -Name '(Default)' -Value $newCommand
        Write-Host "Обновлено HKCR\telemost\shell\open\command -> $newCommand"
    } else {
        Write-Host "Пропускаем обновление HKCR - нет InstallDir."
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
