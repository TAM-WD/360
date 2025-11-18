@echo off
setlocal EnableExtensions

rem === CONFIG ===
set PY_REQUIRED_MAJOR=3
set PY_REQUIRED_MINOR=12
set SCRIPT_NAME=user_activity_checker.py

rem =======================================================================
rem === Check if Python exists and its version matches requirements      ===
rem =======================================================================

for /f "usebackq tokens=2 delims=[]" %%a in (`where python 2^>nul`) do set PY_PATH=%%a
if defined PY_PATH goto CHECK_VERSION

python --version >nul 2>&1
if %errorlevel%==0 goto CHECK_VERSION

goto INSTALL

:CHECK_VERSION
for /f "tokens=2 delims= " %%v in ('python --version 2^>nul') do set VER=%%v
for /f "tokens=1,2 delims=." %%m in ("%VER%") do (
    set MAJOR=%%m
    set MINOR=%%n
)

if "%MAJOR%"=="%PY_REQUIRED_MAJOR%" if "%MINOR%"=="%PY_REQUIRED_MINOR%" goto QUICK_RUN

echo Python found but older version detected: %VER%
goto INSTALL


rem =======================================================================
rem === Install/Update Python silently                                  ===
rem =======================================================================
:INSTALL
echo Installing/Updating Python silently...

if "%PROCESSOR_ARCHITECTURE%"=="AMD64" (
    set ARCH=amd64
) else (
    set ARCH=win32
)

set PY_VERSION_FULL=3.12.6
set PY_URL=https://www.python.org/ftp/python/%PY_VERSION_FULL%/python-%PY_VERSION_FULL%-%ARCH%.exe
set PY_INSTALLER=python-installer.exe

echo Downloading Python %PY_VERSION_FULL% (%ARCH%)...
powershell -Command "(New-Object Net.WebClient).DownloadFile('%PY_URL%', '%PY_INSTALLER%')"

if not exist "%PY_INSTALLER%" (
    echo Failed to download Python installer!
    pause
    exit /b 1
)

echo Checking SHA256...
powershell -Command "$h=Get-FileHash '%PY_INSTALLER%' -Algorithm SHA256; if(!$h.Hash){exit 1}"

if %errorlevel% neq 0 (
    echo Hash check failed!
    del "%PY_INSTALLER%"
    pause
    exit /b 1
)

echo Installing...
"%PY_INSTALLER%" /quiet InstallAllUsers=0 PrependPath=1 Include_pip=1 Include_test=0

if %errorlevel% neq 0 (
    echo Python installation failed!
    pause
    exit /b 1
)

del "%PY_INSTALLER%" >nul 2>&1

python -m pip install --upgrade pip >nul
python -m pip install requests >nul


rem =======================================================================
rem === Always leave console open to show script output                 ===
rem =======================================================================
:QUICK_RUN
python -c "import requests" >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing required Python package: requests...
    python -m pip install requests
)
echo.
echo === Running Python script ===
echo.
python "%~dp0%SCRIPT_NAME%"
echo.
echo Script finished. Console remains open for review.
echo Press any key to exit...
pause >nul
exit /b 0
