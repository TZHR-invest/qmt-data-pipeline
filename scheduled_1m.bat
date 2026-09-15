@echo off
set LOGDIR=G:\qmt_projects\qmt-data-pipeline\logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set LOGFILE=%LOGDIR%\1m_%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%.log
rem === 2026-09-15: data source moved from mini miniquote(58610) to the BigQMT bridge ===
set PYTHONPATH=C:\bridge-client\bridge\src;C:\bridge-client
set BIGQMT_ACCOUNT_ID=666810082889
set BIGQMT_FORMULA_ENABLED=0
echo [%DATE% %TIME%] 1m daily update started >> "%LOGFILE%"
G:\qmt_projects\quant-qmt-proxy\.venv\Scripts\python.exe G:\qmt_projects\qmt-data-pipeline\check_trading_day.py >> "%LOGFILE%" 2>&1
if errorlevel 1 (
    echo [%DATE% %TIME%] not a trading day, skip >> "%LOGFILE%"
    exit /b 0
)
G:\qmt_projects\quant-qmt-proxy\.venv\Scripts\python.exe G:\qmt_projects\qmt-data-pipeline\update_all_1m_mp.py --workers 4 >> "%LOGFILE%" 2>&1
echo [%DATE% %TIME%] 1m daily update finished exit=%ERRORLEVEL% >> "%LOGFILE%"