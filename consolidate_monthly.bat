@echo off
setlocal enabledelayedexpansion

set LOGDIR=G:\qmt_projects\qmt-data-pipeline\logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set LOGFILE=%LOGDIR%\consolidate_%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%.log

for /f %%i in ('powershell -Command "(Get-Date).AddMonths(-1).ToString('yyyy-MM')"') do set MONTH=%%i

echo [%DATE% %TIME%] consolidation started for %MONTH% >> "%LOGFILE%"

G:\qmt_projects\quant-qmt-proxy\.venv\Scripts\python.exe G:\qmt_projects\qmt-data-pipeline\consolidate_1m_to_monthly.py --month %MONTH% --delete >> "%LOGFILE%" 2>&1
set EXITCODE=%ERRORLEVEL%

echo [%DATE% %TIME%] consolidation finished exit=%EXITCODE% >> "%LOGFILE%"

if %EXITCODE% neq 0 (
    echo [%DATE% %TIME%] WARNING: consolidation exited with code %EXITCODE% >> "%LOGFILE%"
    exit /b %EXITCODE%
)

echo [%DATE% %TIME%] consolidation OK - daily files deleted for %MONTH% >> "%LOGFILE%"
